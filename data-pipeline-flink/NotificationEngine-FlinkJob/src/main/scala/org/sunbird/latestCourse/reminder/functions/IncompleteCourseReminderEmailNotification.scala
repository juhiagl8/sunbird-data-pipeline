package org.sunbird.latestCourse.reminder.functions

import org.slf4j.LoggerFactory
import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.Gson
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.sunbird.dp.contentupdater.core.util.RestUtil
import org.sunbird.dp.core.cache.{DataCache, RedisConnect}
import org.sunbird.dp.core.util.CassandraUtil
import org.sunbird.latestCourse.reminder.domain.Event
import org.sunbird.latestCourse.reminder.task.NotificationEngineEmailConfig
import org.sunbird.latestCourse.reminder.util.RestApiUtil

import java.util
import java.util.{Date, Map, Properties}


class IncompleteCourseReminderEmailNotification(config: NotificationEngineEmailConfig)(implicit val mapTypeInfo: TypeInformation[Event]) {

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

  private var dataCache: DataCache = new DataCache(config, new RedisConnect(config.metaRedisHost, config.metaRedisPort, config), config.relationCacheNode, List())
  dataCache.init()
  private var contentCache: DataCache = new DataCache(config, new RedisConnect(config.metaRedisHost, config.metaRedisPort, config), config.contentCacheNode, List())
  contentCache.init()
  private var restUtil: RestUtil = new RestUtil()
  private var restApiUtil: RestApiUtil = new RestApiUtil()
  var cassandraUtil: CassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)

  def initiateIncompleteCourseEmailReminder():Unit= {
    try {
      val date = new Date(new Date().getTime - config.last_access_time_gap_millis)
      val query = QueryBuilder.select().all()
        .from(config.dbCoursesKeyspace, config.USER_CONTENT_DB_TABLE).
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
      val query = QueryBuilder.select().column(config.IDENTIFIER).column(config.HIERARCHY)
        .from(config.dev_hierarchy_store_keyspace, config.content_hierarchy_table)
        .where(QueryBuilder.eq("identifier", cid))
        .allowFiltering().toString
      val row = cassandraUtil.find(query)
      courseId.remove(cid)
      for (i <- 0 to row.size() - 1) {
        val list = row.get(i)
        val courseListMap: java.util.Map[String, Any] = new java.util.HashMap[String, Any]
        courseListMap.put(config.IDENTIFIER, list.getString(config.IDENTIFIER))
        courseListMap.put(config.HIERARCHY, list.getString(config.HIERARCHY))
        val conversion = new Gson().fromJson(list.getString(config.HIERARCHY), classOf[java.util.Map[String, Any]])
        var courseName = ""
        var poster_image = ""
        if (conversion.get(config.NAME) != null) {
          courseName = conversion.get(config.NAME).toString
        }
        if (conversion.get(config.POSTER_IMAGE) != null) {
          poster_image = conversion.get(config.POSTER_IMAGE).toString
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
      val courseUrl = config.COURSE_URL + courseId + config.OVERVIEW_BATCH_ID + batchId
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
    val query = QueryBuilder.select().column(config.EMAIL).from(config.dbSunbirdKeyspace, config.EXCLUDE_USER_EMAILS).allowFiltering().toString
    val excludeEmails = cassandraUtil.find(query)
    val excludeEmailsList: java.util.List[Any] = new java.util.ArrayList[Any]()
    for (i <- 0 to excludeEmails.size() - 1) {
      excludeEmailsList.add(excludeEmails.get(i))
    }
    for (i <- 0 to userIds.size() - 1) {
      val id = userIds.get(i)
      val queryForUserDetails = QueryBuilder.select().column(config.ID).column(config.PROFILE_DETAILS_KEY).from(config.dbSunbirdKeyspace, config.TABLE_USER)
        .where(QueryBuilder.eq("id", id))
        .and(QueryBuilder.eq("isDeleted", isDeleted))
        .and(QueryBuilder.eq("status", 1)).allowFiltering().toString
      val rowData = cassandraUtil.find(queryForUserDetails)
      userDetailsListRow.addAll(rowData)
    }
    for (i <- 0 to userDetailsListRow.size() - 1) {
      val list = userDetailsListRow.get(i)
      try {
        if (list.getString(config.PROFILE_DETAILS_KEY) != null && userMap.get(list.getString(config.ID)) != null) {
          val profiledetails: String = list.getString(config.PROFILE_DETAILS_KEY)
          var hashMap: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
          hashMap = new ObjectMapper().readValue(profiledetails, classOf[java.util.HashMap[String, Any]])
          val personalDetailsKey = hashMap.get(config.PERSONAL_DETAILS_KEY)
          val personalDetailsMap: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
          personalDetailsMap.putAll(personalDetailsKey.asInstanceOf[java.util.HashMap[String, Any]])
          if (personalDetailsMap.get(config.PRIMARY_EMAIL) != null && !excludeEmailsList.contains(personalDetailsMap.get(config.PRIMARY_EMAIL))) {
            val primaryEmail: String = personalDetailsMap.get(config.PRIMARY_EMAIL).toString
            val charArrays = primaryEmail.toCharArray
            val stringBuilder: StringBuilder = new StringBuilder()
            for (i <- 0 to charArrays.length() - 1) {
              stringBuilder.append(primaryEmail.charAt(i))
            }
            val userId = list.getString(config.ID)
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
          params.put(config.COURSE_KEYWORD + j, true)
          params.put(config.COURSE_KEYWORD + j + config._URL, userCourseProgressDetailsEntry.getValue.incompleteCourses.get(i).courseUrl)
          params.put(config.COURSE_KEYWORD + j + config.THUMBNAIL, userCourseProgressDetailsEntry.getValue.incompleteCourses.get(i).thumbnail)
          params.put(config.COURSE_KEYWORD + j + config._NAME, userCourseProgressDetailsEntry.getValue.incompleteCourses.get(i).courseName)
          params.put(config.COURSE_KEYWORD + j + config._DURATION, userCourseProgressDetailsEntry.getValue.incompleteCourses.get(i).completionPercentage)
        }
        logger.info("params "+params)
        /*val kafkaProducerProps = new Properties()
        kafkaProducerProps.put("bootstrap.servers", config.BOOTSTRAP_SERVER_CONFIG)
        kafkaProducerProps.put("key.serializer", classOf[StringSerializer].getName)
        kafkaProducerProps.put("value.serializer", classOf[StringSerializer].getName)
        val producer = new KafkaProducer[String, String](kafkaProducerProps)
        val producerData = new util.HashMap[String, Any]
        producerData.put(config.EMAILS, userCourseProgressDetailsEntry.getValue.email)
        producerData.put(config.PARAMS, params)
        producerData.put(config.emailTemplate, config.INCOMPLETE_COURSES)
        producerData.put(config.emailSubject, config.INCOMPLETE_COURSES_MAIL_SUBJECT)
        producerData.put("senderMail", config.SENDER_MAIL)
        producer.send(new ProducerRecord[String, String](config.kafkaOutPutStreamTopic, config.DATA, producerData.toString))*/
        sendNotification(java.util.Collections.singletonList(userCourseProgressDetailsEntry.getValue.email), params, config.SENDER_MAIL, config.NOTIFICATION_HOST + config.notification_event_endpoint, config.INCOMPLETE_COURSES, config.INCOMPLETE_COURSES_MAIL_SUBJECT)
        set.remove(userCourseProgressDetailsEntry)
      }
    } catch {
      case e: Exception => e.printStackTrace()
        logger.info(String.format("Error in send notification %s", e.getMessage()))
    }
  }

  def sendNotification(sendTo: java.util.List[String], params: java.util.Map[String, Any], senderMail: String, notificationUrl: String, emailTemplate: String, emailSubject: String): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        try {
          val notificationTosend: java.util.List[Any] = new java.util.ArrayList[Any](java.util.Arrays.asList(new Notification(config.EMAIL, config.MESSAGE, new EmailConfig(sender = senderMail, subject = emailSubject),
            ids = sendTo, new Template(data = null, id = emailTemplate, params = params))));
          val notificationRequest: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
          notificationRequest.put(config.REQUEST, new java.util.HashMap[String, java.util.List[Any]]() {
            {
              put(config.NOTIFICATIONS, notificationTosend)
            }
          })
          restApiUtil.postRequestForSendingMail(notificationUrl, notificationRequest)
        } catch {
          case e: Exception => e.printStackTrace()
            logger.info(String.format("Failed during sending mail %s", e.getMessage()))
        }
      }
    }).start()
  }
}
