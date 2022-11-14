package org.sunbird.notificationEngine.functions

import org.slf4j.LoggerFactory
import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.Gson
import org.apache.commons.collections.MapUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.sunbird.dp.contentupdater.core.util.RestUtil
import org.sunbird.dp.core.cache.{DataCache, RedisConnect}
import org.sunbird.dp.core.util.CassandraUtil
import org.sunbird.notificationEngine.domain.Event
import org.sunbird.notificationEngine.task.NotificationEngineEmailConfig
import org.sunbird.notificationEngine.util.RestApiUtil

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util
import java.util.{Date, Map, Properties, UUID}


class IncompleteCourseReminderEmailNotification(notificationConfig: NotificationEngineEmailConfig)(implicit val mapTypeInfo: TypeInformation[Event]) {

  case class CoursesDataMap(courseId: String, courseName: String, batchId: String, completionPercentage: Float, lastAccessedDate: java.util.Date, thumbnail: String, courseUrl: String, duration: String, description: String)

  case class CourseDetails(courseName: String, thumbnail: String)

  case class UserCourseProgressDetails(email: String, incompleteCourses: java.util.List[CoursesDataMap])

  case class EmailConfig(sender: String, subject: String)

  case class Notification(mode: String, deliveryType: String, config: EmailConfig, ids: java.util.List[String], template: Template)

  case class Template(data: String, id: String, params: java.util.Map[String, Any])

  var userCourseMap: java.util.Map[String, UserCourseProgressDetails] = new java.util.HashMap[String, UserCourseProgressDetails]()
  val courseIdAndCourseNameMap: java.util.Map[String, CourseDetails] = new java.util.HashMap[String, CourseDetails]()

  val incompleteCourse: java.util.List[CoursesDataMap] = new java.util.ArrayList[CoursesDataMap]()

  private[this] val logger = LoggerFactory.getLogger(classOf[IncompleteCourseReminderEmailNotification])

  private var dataCache: DataCache = new DataCache(notificationConfig, new RedisConnect(notificationConfig.metaRedisHost, notificationConfig.metaRedisPort, notificationConfig), notificationConfig.relationCacheNode, List())
  dataCache.init()
  private var contentCache: DataCache = new DataCache(notificationConfig, new RedisConnect(notificationConfig.metaRedisHost, notificationConfig.metaRedisPort, notificationConfig), notificationConfig.contentCacheNode, List())
  contentCache.init()
  private var restUtil: RestUtil = new RestUtil()
  private var restApiUtil: RestApiUtil = new RestApiUtil()
  var cassandraUtil: CassandraUtil = new CassandraUtil(notificationConfig.dbHost, notificationConfig.dbPort)

  def initiateIncompleteCourseEmailReminder():Unit= {
    try {
      val date = new Date(new Date().getTime - notificationConfig.last_access_time_gap_millis)
      val query = QueryBuilder.select().all()
        .from(notificationConfig.dbCoursesKeyspace, notificationConfig.USER_CONTENT_DB_TABLE).
        where(QueryBuilder.gt("completionpercentage", 0))
        .and(QueryBuilder.lt("completionpercentage", 100))
        .and(QueryBuilder.gt("last_access_time", 0))
        .and(QueryBuilder.lt("last_access_time", date))
        .allowFiltering().toString
      val rows: java.util.List[Row] = cassandraUtil.find(query)
      if (rows != null) {
        fetchCourseIdsAndSetCourseNameAndThumbnail(rows)
        setUserCourseMap(rows, userCourseMap)
        getAndSetUserEmail(userCourseMap)
        var set = userCourseMap.entrySet()
        for (i <- 0 to set.size() - 1) {
          sendIncompleteCourseEmail(set)
        }
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        logger.info(s"Getting Incomplete Courses Details Failed with exception ${ex.getMessage}:")
    }
  }

  def fetchCourseIdsAndSetCourseNameAndThumbnail(userCourseList: java.util.List[Row]): Unit = {
    var courseId: java.util.Set[String] = new java.util.HashSet[String]()
    for (i <- 0 to userCourseList.size() - 1) {
      val list = userCourseList.get(i)
      val cid = list.getString("courseid")
      courseId.add(cid)
    }
    getAndSetCourseName(courseId)
  }

  def getAndSetCourseName(courseId: java.util.Set[String]): Unit = {
    for (i <- 0 to courseId.size() - 1) {
      var cid = ""
      courseId.forEach(id => cid = id)
      val identifier = cid
      val query = QueryBuilder.select().column(notificationConfig.IDENTIFIER).column(notificationConfig.HIERARCHY)
        .from(notificationConfig.dev_hierarchy_store_keyspace, notificationConfig.content_hierarchy_table)
        .where(QueryBuilder.eq("identifier", cid))
        .allowFiltering().toString
      val row = cassandraUtil.find(query)
      courseId.remove(cid)
      for (i <- 0 to row.size() - 1) {
        val list = row.get(i)
        val courseListMap: java.util.Map[String, Any] = new java.util.HashMap[String, Any]
        courseListMap.put(notificationConfig.IDENTIFIER, list.getString(notificationConfig.IDENTIFIER))
        courseListMap.put(notificationConfig.HIERARCHY, list.getString(notificationConfig.HIERARCHY))
        val conversion = new Gson().fromJson(list.getString(notificationConfig.HIERARCHY), classOf[java.util.Map[String, Any]])
        var courseName = ""
        var poster_image = ""
        if (conversion.get(notificationConfig.NAME) != null) {
          courseName = conversion.get(notificationConfig.NAME).toString
        }
        if (conversion.get(notificationConfig.POSTER_IMAGE) != null) {
          poster_image = conversion.get(notificationConfig.POSTER_IMAGE).toString
        }
        val courseDetails = CourseDetails(courseName, poster_image)
        courseIdAndCourseNameMap.put(identifier, courseDetails)
      }
    }
  }

  def setUserCourseMap(userCourseList: java.util.List[Row], userMap: java.util.Map[String, UserCourseProgressDetails]): Unit = {
    for (i <- 0 to userCourseList.size() - 1) {
      val list = userCourseList.get(i)
      val courseId = list.getString("courseid")
      val batchId = list.getString("batchid")
      val userid = list.getString("userid")
      val per = list.getFloat("completionPercentage")
      val lastAccessedDate = list.getTimestamp("last_access_time")
      val courseUrl = notificationConfig.COURSE_URL + courseId + notificationConfig.OVERVIEW_BATCH_ID + batchId
      if (courseId != null && batchId != null && courseIdAndCourseNameMap.get(courseId) != null && courseIdAndCourseNameMap.get(courseId).thumbnail != null) {
        val coursesDataMap = CoursesDataMap(courseId, courseIdAndCourseNameMap.get(courseId).courseName, batchId, per, lastAccessedDate, courseIdAndCourseNameMap.get(courseId).thumbnail, courseUrl, StringUtils.EMPTY, StringUtils.EMPTY)
        if (userMap.get(userid) != null) {
          if (userMap.get(userid).incompleteCourses.size() < 3) {
            userMap.get(userid).incompleteCourses.add(coursesDataMap)
            //TODO
            if (userCourseMap.get(userid).incompleteCourses.size() == 3) {
              var userMapDetailsById = userMap.get(userid)
              var lastAccessDateInRev = new Date()
              for (i <- userCourseList.size() - 1 to 0) {
                lastAccessDateInRev = coursesDataMap.lastAccessedDate
              }
              val coursesData = CoursesDataMap(courseId, courseIdAndCourseNameMap.get(courseId).courseName, batchId, per, lastAccessDateInRev, courseIdAndCourseNameMap.get(courseId).thumbnail, courseUrl, StringUtils.EMPTY, StringUtils.EMPTY)
              userMapDetailsById.incompleteCourses.add(coursesData)
              userMap.put(userid, userMapDetailsById)
            }
          }
        } else {
          incompleteCourse.add(coursesDataMap)
          var userCourseProgressDetails = UserCourseProgressDetails("email", new java.util.ArrayList[CoursesDataMap])
          userCourseProgressDetails.incompleteCourses.addAll(incompleteCourse)
          userMap.put(userid, userCourseProgressDetails)
        }
      }
    }
  }

  def getAndSetUserEmail(userMap: java.util.Map[String, UserCourseProgressDetails]): Unit = {
    val userIds: java.util.List[String] = new java.util.ArrayList[String]()
    var userDetailsListRow: java.util.List[Row] = new java.util.ArrayList[Row]()
    val isDeleted = false
    userIds.addAll(userMap.keySet())
    val query = QueryBuilder.select().column(notificationConfig.EMAIL).from(notificationConfig.dbSunbirdKeyspace, notificationConfig.EXCLUDE_USER_EMAILS).allowFiltering().toString
    val excludeEmails = cassandraUtil.find(query)
    val excludeEmailsList: java.util.List[Any] = new java.util.ArrayList[Any]()
    for (i <- 0 to excludeEmails.size() - 1) {
      excludeEmailsList.add(excludeEmails.get(i))
    }
    for (i <- 0 to userIds.size() - 1) {
      val id = userIds.get(i)
      val queryForUserDetails = QueryBuilder.select().column(notificationConfig.ID).column(notificationConfig.PROFILE_DETAILS_KEY).from(notificationConfig.dbSunbirdKeyspace, notificationConfig.TABLE_USER)
        .where(QueryBuilder.eq("id", id))
        .and(QueryBuilder.eq("isDeleted", isDeleted))
        .and(QueryBuilder.eq("status", 1)).allowFiltering().toString
      val rowData = cassandraUtil.find(queryForUserDetails)
      userDetailsListRow.addAll(rowData)
    }
    for (i <- 0 to userDetailsListRow.size() - 1) {
      val list = userDetailsListRow.get(i)
      try {
        if (list.getString(notificationConfig.PROFILE_DETAILS_KEY) != null && userMap.get(list.getString(notificationConfig.ID)) != null) {
          val profiledetails: String = list.getString(notificationConfig.PROFILE_DETAILS_KEY)
          var hashMap: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
          hashMap = new ObjectMapper().readValue(profiledetails, classOf[java.util.HashMap[String, Any]])
          val personalDetailsKey = hashMap.get(notificationConfig.PERSONAL_DETAILS_KEY)
          val personalDetailsMap: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
          personalDetailsMap.putAll(personalDetailsKey.asInstanceOf[java.util.HashMap[String, Any]])
          if (personalDetailsMap.get(notificationConfig.PRIMARY_EMAIL) != null && !excludeEmailsList.contains(personalDetailsMap.get(notificationConfig.PRIMARY_EMAIL))) {
            val primaryEmail: String = personalDetailsMap.get(notificationConfig.PRIMARY_EMAIL).toString
            val charArrays = primaryEmail.toCharArray
            val stringBuilder: StringBuilder = new StringBuilder()
            for (i <- 0 to charArrays.length() - 1) {
              stringBuilder.append(primaryEmail.charAt(i))
            }
            val userId = list.getString(notificationConfig.ID)
            var userMapDetails = userMap.get(userId)
            val mail = userMapDetails.copy(email = primaryEmail)
            userMap.put(userId, mail)
          }
        }
      } catch {
        case e: Exception => e.printStackTrace()
          logger.info(String.format("Error in get and set user email %s", e.getMessage()))
      }
    }
  }


  def sendIncompleteCourseEmail(set: util.Set[Map.Entry[String, UserCourseProgressDetails]]): Unit = {
    var userCourseProgressDetailsEntry: java.util.Map.Entry[String, UserCourseProgressDetails] = null
    set.forEach(i => {
      userCourseProgressDetailsEntry = i
    })
    try {
      if (!StringUtils.isEmpty(userCourseProgressDetailsEntry.getValue.email) && userCourseProgressDetailsEntry.getValue.incompleteCourses.size() > 0) {
        val params: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
        for (i <- 0 to userCourseProgressDetailsEntry.getValue.incompleteCourses.size() - 1) {
          val j = i + 1
          params.put(notificationConfig.COURSE_KEYWORD + j, true)
          params.put(notificationConfig.COURSE_KEYWORD + j + notificationConfig._URL, userCourseProgressDetailsEntry.getValue.incompleteCourses.get(i).courseUrl)
          params.put(notificationConfig.COURSE_KEYWORD + j + notificationConfig.THUMBNAIL, userCourseProgressDetailsEntry.getValue.incompleteCourses.get(i).thumbnail)
          params.put(notificationConfig.COURSE_KEYWORD + j + notificationConfig._NAME, userCourseProgressDetailsEntry.getValue.incompleteCourses.get(i).courseName)
          params.put(notificationConfig.COURSE_KEYWORD + j + notificationConfig._DURATION, userCourseProgressDetailsEntry.getValue.incompleteCourses.get(i).completionPercentage)
        }
        initiateKafkaMessage(java.util.Collections.singletonList(userCourseProgressDetailsEntry.getValue.email),notificationConfig.INCOMPLETE_COURSES,params,notificationConfig.INCOMPLETE_COURSES_MAIL_SUBJECT)
        set.remove(userCourseProgressDetailsEntry)
      }
    } catch {
      case e: Exception => e.printStackTrace()
        logger.info(String.format("Error in send notification %s", e.getMessage()))
    }
  }

  //TODO-RestCall are temporarily stopped
  /*def sendNotification(sendTo: java.util.List[String], params: java.util.Map[String, Any], senderMail: String, notificationUrl: String, emailTemplate: String, emailSubject: String): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        try {
          val notificationTosend: java.util.List[Any] = new java.util.ArrayList[Any](java.util.Arrays.asList(new Notification(notificationConfig.EMAIL, notificationConfig.MESSAGE, new EmailConfig(sender = senderMail, subject = emailSubject),
            ids = sendTo, new Template(data = null, id = emailTemplate, params = params))));
          val notificationRequest: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
          notificationRequest.put(notificationConfig.REQUEST, new java.util.HashMap[String, java.util.List[Any]]() {
            {
              put(notificationConfig.NOTIFICATIONS, notificationTosend)
            }
          })
          restApiUtil.postRequestForSendingMail(notificationUrl, notificationRequest)
        } catch {
          case e: Exception => e.printStackTrace()
            logger.info(String.format("Failed during sending mail %s", e.getMessage()))
        }
      }
    }).start()
  }*/

  def initiateKafkaMessage(emailList: util.List[String], emailTemplate: String, params: util.Map[String, Any], EMAIL_SUBJECT: String) = {
    logger.info("Entering InitiateKafkaMessage")
    val Actor = new util.HashMap[String, String]()
    Actor.put(notificationConfig.ID, notificationConfig.BROAD_CAST_TOPIC_NOTIFICATION_MESSAGE)
    Actor.put(notificationConfig.TYPE, notificationConfig.ACTOR_TYPE_VALUE)
    val eid = notificationConfig.EID_VALUE
    val edata = new util.HashMap[String, Any]()
    edata.put(notificationConfig.ACTION, notificationConfig.BROAD_CAST_TOPIC_NOTIFICATION_KEY)
    edata.put(notificationConfig.iteration, 1)
    val request = new util.HashMap[String, Any]()
    val config = new util.HashMap[String, String]()
    config.put(notificationConfig.SENDER, notificationConfig.SENDER_MAIL)
    config.put(notificationConfig.TOPIC, null)
    config.put(notificationConfig.OTP, null)
    config.put(notificationConfig.SUBJECT, EMAIL_SUBJECT)

    val templates = new util.HashMap[String, Any]()
    templates.put(notificationConfig.DATA, null)
    templates.put(notificationConfig.ID, emailTemplate)
    templates.put(notificationConfig.PARAMS, params)
    logger.info("templates " + templates)

    val notification = new util.HashMap[String, Any]()
    notification.put(notificationConfig.rawData, null)
    notification.put(notificationConfig.CONFIG, config)
    notification.put(notificationConfig.DELIVERY_TYPE, notificationConfig.MESSAGE)
    notification.put(notificationConfig.DELIVERY_MODE, notificationConfig.EMAIL)
    notification.put(notificationConfig.TEMPLATE, templates)
    notification.put(notificationConfig.IDS, emailList)

    logger.info("notification " + notification)

    request.put(notificationConfig.NOTIFICATION, notification)
    edata.put(notificationConfig.REQUEST, request)
    val trace = new util.HashMap[String, Any]()
    trace.put(notificationConfig.X_REQUEST_ID, null)
    trace.put(notificationConfig.X_TRACE_ENABLED, false)
    val pdata = new util.HashMap[String, Any]()
    pdata.put(notificationConfig.VER, "1.0")
    pdata.put(notificationConfig.ID, "org.sunbird.platform")
    val context = new util.HashMap[String, Any]()
    context.put(notificationConfig.PDATA, pdata)
    val ets = System.currentTimeMillis()
    val mid = notificationConfig.PRODUCER_ID + "." + ets + "." + UUID.randomUUID();
    val objectsDetails = new util.HashMap[String, Any]()
    objectsDetails.put(notificationConfig.ID, getRequestHashed(request, context))
    objectsDetails.put(notificationConfig.TYPE, notificationConfig.TYPE_VALUE)

    val producerData = new util.HashMap[String, Any]
    producerData.put(notificationConfig.ACTOR, Actor)
    producerData.put(notificationConfig.EDATA, edata)
    producerData.put(notificationConfig.EID, eid)
    producerData.put(notificationConfig.TRACE, trace)
    producerData.put(notificationConfig.CONTEXT, context)
    producerData.put(notificationConfig.MID, mid)
    producerData.put(notificationConfig.OBJECT, objectsDetails)

    sendMessageToKafkaTopic(producerData)
  }

  def getRequestHashed(request: util.HashMap[String, Any], context: util.HashMap[String, Any]): String = {
    var value = new String()
    try {
      val mapper: ObjectMapper = new ObjectMapper()
      val mapValue = mapper.writeValueAsString(request)
      val md = MessageDigest.getInstance("SHA-256")
      md.update(mapValue.getBytes(StandardCharsets.UTF_8))
      val byteData = md.digest
      val sb = new StringBuilder()
      for (i <- 0 to byteData.length - 1) {
        sb.append(Integer.toString((byteData(i) & 0xff) + 0x100, 16).substring(1))
      }
      value = sb.toString()
    } catch {
      case e: Exception => e.printStackTrace()
        logger.error("Error while encrypting " + context, e);
        value = ""
    }
    value
  }

  def sendMessageToKafkaTopic(producerData: util.HashMap[String, Any]): Unit = {
    logger.info("Entering SendMessageKafkaTopic")
    if (MapUtils.isNotEmpty(producerData)) {
      val kafkaProducerProps = new Properties()
      kafkaProducerProps.put(notificationConfig.bootstrap_servers, notificationConfig.BOOTSTRAP_SERVER_CONFIG)
      kafkaProducerProps.put(notificationConfig.key_serializer, classOf[StringSerializer])
      kafkaProducerProps.put(notificationConfig.value_serializer, classOf[StringSerializer])
      val producer = new KafkaProducer[String, String](kafkaProducerProps)
      val mapper: ObjectMapper = new ObjectMapper()
      mapper.setVisibility(PropertyAccessor.ALL, Visibility.ANY)
      val jsonString = mapper.writeValueAsString(producerData)
      producer.send(new ProducerRecord[String, String](notificationConfig.NOTIFICATION_JOB_Topic, notificationConfig.DATA, jsonString))
    }
  }
}
