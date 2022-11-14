package org.sunbird.notificationEngine.functions

import com.datastax.driver.core.querybuilder.QueryBuilder
import org.elasticsearch.index.query._
import com.google.gson.Gson
import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.search.{SearchHit, SearchHits}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.slf4j.LoggerFactory
import org.sunbird.dp.contentupdater.core.util.RestUtil
import org.sunbird.dp.core.cache.{DataCache, RedisConnect}
import org.sunbird.dp.core.serde.StringSerializationSchema
import org.sunbird.dp.core.util.CassandraUtil
import org.sunbird.notificationEngine.domain.Event
import org.sunbird.notificationEngine.task.NotificationEngineEmailConfig
import org.sunbird.notificationEngine.util.{IndexService, RestApiUtil}

import java.io.StringWriter
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.LocalDate
import java.util
import java.util.Map.Entry
import java.util.concurrent.CompletableFuture
import java.util.{Arrays, Collections, Date, Properties, UUID}


class LatestCourseEmailNotificationFunction(courseConfig: NotificationEngineEmailConfig)(implicit val mapTypeInfo: TypeInformation[Event]) {


  case class NewCourseData(id: String, ver: String, ts: String, params: Params, responseCode: String, result: Result)

  case class Params(resmsgid: String, msgid: String, status: String, err: Any, errmsg: Any)

  case class Result(count: Int, content: java.util.List[Content] = null)

  case class Content(trackable: Trackable, instructions: String, identifier: String, purpose: String, channel: String, organisation: java.util.List[String] = null,
                     description: String, creatorLogo: String, mimeType: String, posterImage: String, idealScreenSize: String, version: Int, pkgVersion: Int,
                     objectType: String, learningMode: String, duration: String, license: String, appIcon: String, primaryCategory: String, name: String, lastUpdatedOn: String, contentType: String)

  case class Trackable(enabled: String, autoBatch: String)

  case class CoursesDataMap(courseId: String, courseName: String, thumbnail: String, courseUrl: String, duration: Int, description: String)


  case class Notification(mode: String, deliveryType: String, config: EmailConfig, ids: java.util.List[String], template: Templates)

  case class Templates(data: String, id: String, params: java.util.Map[String, Any])

  case class EmailConfig(sender: String, subject: String)

  private[this] val logger = LoggerFactory.getLogger(classOf[LatestCourseEmailNotificationFunction])

  private var dataCache: DataCache = new DataCache(courseConfig, new RedisConnect(courseConfig.metaRedisHost, courseConfig.metaRedisPort, courseConfig), courseConfig.relationCacheNode, List())
  dataCache.init()
  private var contentCache: DataCache = new DataCache(courseConfig, new RedisConnect(courseConfig.metaRedisHost, courseConfig.metaRedisPort, courseConfig), courseConfig.contentCacheNode, List())
  contentCache.init()
  private var restUtil: RestUtil =  new RestUtil()
  private var restApiUtil: RestApiUtil =  new RestApiUtil()
  var cassandraUtil: CassandraUtil = new CassandraUtil(courseConfig.dbHost, courseConfig.dbPort)

  def initiateLatestCourseAlertEmail(): Unit = {
    val newCourseData: NewCourseData = getLatestAddedCourses()
    if (newCourseData != null && newCourseData.result.content.size() >= courseConfig.latest_courses_alert_content_min_limit) {
      val coursesDataMapList: util.List[CoursesDataMap] = setCourseMap(newCourseData.result.content)
      if (sendNewCourseEmail(coursesDataMapList)) {
        updateEmailRecordInTheDatabase();
      }
    } else {
      logger.info("There are no latest courses or number of latest courses are less than " + courseConfig.latest_courses_alert_content_min_limit)
    }
  }

  def getLatestAddedCourses(): NewCourseData = {
    logger.info("Entering getLatestAddedCourses")
    try {
      val lastUpdatedOn = new util.HashMap[String, Any]()
      val maxValue = LocalDate.now()
      lastUpdatedOn.put(courseConfig.MIN, calculateMinValue(maxValue))
      lastUpdatedOn.put(courseConfig.MAX, maxValue.toString)
      val filters = new util.HashMap[String, Any]()
      filters.put(courseConfig.PRIMARY_CATEGORY, Collections.singletonList(courseConfig.COURSE))
      filters.put(courseConfig.CONTENT_TYPE_SEARCH, Collections.singletonList(courseConfig.COURSE))
      filters.put(courseConfig.LAST_UPDATED_ON, lastUpdatedOn)
      val sortBy = new util.HashMap[String, Any]()
      sortBy.put(courseConfig.LAST_UPDATED_ON, courseConfig.DESCENDING_ORDER)
      val searchFields = courseConfig.SEARCH_FIELDS
      val request = new util.HashMap[String, Any]()
      request.put(courseConfig.FILTERS, filters)
      request.put(courseConfig.OFFSET, 0)
      request.put(courseConfig.LIMIT, 1000)
      request.put(courseConfig.SORT_BY, sortBy)
      request.put(courseConfig.FIELDS, searchFields.split(",", -1))
      val requestBody = new util.HashMap[String, Any]()
      requestBody.put(courseConfig.REQUEST, request)
      if (!lastUpdatedOn.get(courseConfig.MAX).toString.equalsIgnoreCase(lastUpdatedOn.get(courseConfig.MIN).toString)) {
        val url: String = courseConfig.KM_BASE_HOST + courseConfig.content_search
        val obj = fetchResultUsingPost(url, requestBody)
        val gson = new Gson()
        return gson.fromJson(obj, classOf[NewCourseData])
      }
    } catch {
      case e: Exception => e.printStackTrace()
        logger.info(String.format("Error in get and set user email %s", e.getMessage()))
    }
    return null
  }

  def calculateMinValue(maxValue: LocalDate): String = {
    var minValue: String = ""
    val query = QueryBuilder.select().column(courseConfig.LAST_SENT_DATE)
      .from(courseConfig.dbSunbirdKeyspace, courseConfig.EMAIL_RECORD_TABLE)
      .where(QueryBuilder.eq(courseConfig.EMAIL_TYPE, courseConfig.NEW_COURSES_EMAIL)).allowFiltering().toString
    val emailRecords = cassandraUtil.find(query)
    if (!emailRecords.isEmpty) {
      if (!StringUtils.isEmpty(emailRecords.get(0).getString(courseConfig.LAST_SENT_DATE))) {
        minValue = emailRecords.get(0).getString(courseConfig.LAST_SENT_DATE)
      } else {
        minValue = ""
      }
    }
    if (StringUtils.isEmpty(minValue)) {
      minValue = maxValue.minusDays(courseConfig.new_courses_scheduler_time_gap / 24).toString
    }
    minValue
  }

  def fetchResultUsingPost(uri: String, requestBody: util.Map[String, Any]): String = {
    var response = new String()
    try {
      response = restApiUtil.postRequestWithContentType(uri, requestBody)
    } catch {
      case e: Exception => e.printStackTrace()
        logger.info("Error received: " + e.getMessage())
        response = e.getMessage
    }
    response
  }

  def setCourseMap(courseList: util.List[Content]): util.List[CoursesDataMap] = {
    logger.info("Entering setCourseMap")
    val coursesDataMapList = new util.ArrayList[CoursesDataMap]()
    for (i <- 0 to courseList.size() - 1 if i < courseConfig.new_courses_email_limit) {
      try {
        val Id: String = courseList.get(i).identifier
        if (!StringUtils.isEmpty(courseList.get(i).identifier) && !StringUtils.isEmpty(courseList.get(i).name) && !StringUtils.isEmpty(courseList.get(i).posterImage) && !StringUtils.isEmpty(courseList.get(i).duration)) {
          val courseName = courseList.get(i).name.toLowerCase.capitalize
          val thumbnail = courseList.get(i).posterImage
          val courseUrl = courseConfig.COURSE_URL + Id
          val description = courseList.get(i).description
          val duration = courseList.get(i).duration.toInt
          val coursesDataMap = new CoursesDataMap(Id, courseName, thumbnail, courseUrl, duration, description)
          coursesDataMapList.add(coursesDataMap)
        }
      } catch {
        case e: Exception => e.printStackTrace()
          logger.info("Error while set course : " + e.getMessage())
      }
    }
    coursesDataMapList
  }


  def sendNewCourseEmail(coursesDataMapList: util.List[CoursesDataMap]): Boolean = {
    try {
      logger.info("Entering new courses email")
      val params = new util.HashMap[String, Any]()
      params.put(courseConfig.NO_OF_COURSES, coursesDataMapList.size())
      for (i <- 0 to coursesDataMapList.size() - 1 if i < courseConfig.new_courses_email_limit) {
        val j: Int = i + 1;
        params.put(courseConfig.COURSE_KEYWORD + j, true)
        params.put(courseConfig.COURSE_KEYWORD + j + courseConfig._URL, coursesDataMapList.get(i).courseUrl)
        params.put(courseConfig.COURSE_KEYWORD + j + courseConfig.THUMBNAIL, coursesDataMapList.get(i).thumbnail)
        params.put(courseConfig.COURSE_KEYWORD + j + courseConfig._NAME, coursesDataMapList.get(i).courseName)
        params.put(courseConfig.COURSE_KEYWORD + j + courseConfig._DURATION, convertSecondsToHrsAndMinutes(coursesDataMapList.get(i).duration))
        params.put(courseConfig.COURSE_KEYWORD + j + courseConfig._DESCRIPTION, coursesDataMapList.get(i).description)
      }
      val isEmailSentToConfigMailIds: Boolean = sendEmailsToConfigBasedMailIds(params)
      var isEmailSentToESMailIds: Boolean = false
      if (courseConfig.latest_courses_alert_send_to_all_user) {
        val query = QueryBuilder.select().column(courseConfig.EMAIL)
          .from(courseConfig.dbSunbirdKeyspace, courseConfig.EXCLUDE_USER_EMAILS)
          .allowFiltering().toString
        val excludeEmails = cassandraUtil.find(query)
        val excludeEmailsList: java.util.List[Any] = new java.util.ArrayList[Any]()
        for (i <- 0 to excludeEmails.size() - 1) {
          excludeEmailsList.add(excludeEmails.get(i))
        }
        isEmailSentToESMailIds = fetchEmailIdsFromUserES(excludeEmailsList, params)
      }
      return isEmailSentToConfigMailIds
    } catch {
      case e: Exception => e.printStackTrace()
        logger.error("Error while sending New Course Email : " + e.getMessage())
    }
    return false
  }

  def convertSecondsToHrsAndMinutes(seconds: Int): String = {
    var time: String = ""
    if (seconds > 60) {
      val min: Int = (seconds / 60) % 60
      val hours: Int = (seconds / 60) / 60
      var minutes = new String()
      if (min < 10) {
        minutes = "0" + min
      } else {
        minutes = min.toString
      }
      var strHours = new String()
      if (hours < 10) {
        strHours = "0" + hours
      } else {
        strHours = hours.toString
      }
      if (min > 0 && hours > 0) {
        time = strHours + "h " + minutes + "m"
      } else if (min == 0 && hours > 0) {
        time = strHours + "h"
      } else if (min > 0) {
        time = minutes + "m"
      }
    }
    return time
  }

  def sendEmailsToConfigBasedMailIds(params: util.HashMap[String, Any]): Boolean = {
    logger.info("Entering sendEmailsToConfigBasedMailIds")
    try {
      val mailList: util.List[String] = new util.ArrayList[String]()
      val mails: util.List[String] = new util.ArrayList[String]()
      val mail = courseConfig.MAIL_LIST.split(",", -1)
      mail.foreach(i => mails.add(i))
      mails.forEach(m => mailList.add(m))
      sendNotification(mailList,params,courseConfig.SENDER_MAIL,courseConfig.notification_service_host+courseConfig.notification_event_endpoint,courseConfig.NEW_COURSES,courseConfig.NEW_COURSES_MAIL_SUBJECT)
      return true
    } catch {
      case e: Exception => e.printStackTrace()
        logger.info(String.format("Failed during sending mail %s", e.getMessage()))
    }
    return true
  }

  def fetchEmailIdsFromUserES(excludeEmailsList: util.List[Any], params: util.Map[String, Any]): Boolean = {
    val resultArray = new util.ArrayList[util.HashMap[String, Any]]
    var result = new util.HashMap[String, Any]()
    logger.info("Entering fetchEmailIdsFromUserES")
    try {
      var count: Int = 1
      val limit: Int = 45
      var offset: Int = 0
      var response = new SearchResponse()
      while (offset < count) {
        val emailWithUserId=new util.ArrayList[util.HashMap[String,Any]]
        val query: BoolQueryBuilder = QueryBuilders.boolQuery()
        val finalQuery: BoolQueryBuilder = QueryBuilders.boolQuery()
        finalQuery.must(QueryBuilders.matchQuery(courseConfig.STATUS, 1))
          .must(QueryBuilders.matchQuery(courseConfig.IS_DELETED, false)).must(query)
        val sourceBuilder = new SearchSourceBuilder().query(finalQuery)
        val excludeFields=new Array[String](0)
        sourceBuilder.fetchSource(courseConfig.fields.split(",",-1),excludeFields)
        sourceBuilder.from(offset)
        sourceBuilder.size(45)
        val index = new IndexService(courseConfig)
        response = index.getEsResult(courseConfig.sb_es_user_profile_index, courseConfig.es_profile_index_type, sourceBuilder, true)
        response.getHits.forEach(hitDetails => {
          result = hitDetails.getSourceAsMap().asInstanceOf[util.HashMap[String, Any]]
          val userId = result.get(courseConfig.USERID).toString
          if (result.containsKey(courseConfig.PROFILE_DETAILS)) {
            val profileDetails: util.HashMap[String, Any] = result.get(courseConfig.PROFILE_DETAILS).asInstanceOf[util.HashMap[String, Any]]
            if (profileDetails.containsKey(courseConfig.PERSONAL_DETAILS)) {
              val personalDetails: util.HashMap[String, Any] = profileDetails.get(courseConfig.PERSONAL_DETAILS).asInstanceOf[util.HashMap[String, Any]]
              if (MapUtils.isNotEmpty(personalDetails)) {
                val email: String = personalDetails.get(courseConfig.PRIMARY_EMAIL).asInstanceOf[String]
                if (StringUtils.isNotBlank(email) && !excludeEmailsList.contains(email)) {
                  if (courseConfig.MAIL_LIST != null && !courseConfig.MAIL_LIST.contains(email)) {
                    val emailWithUserIdMap = new util.HashMap[String,Any]()
                    emailWithUserIdMap.put(userId,email)
                    emailWithUserId.add(emailWithUserIdMap)
                  } else {
                    logger.info("Invalid Email :" + email)
                  }
                }
              }
            }
          }
        })
        if (CollectionUtils.isNotEmpty(emailWithUserId)) {
          val kafkaProducerProps = new Properties()
          kafkaProducerProps.put(courseConfig.bootstrap_servers, courseConfig.BOOTSTRAP_SERVER_CONFIG)
          kafkaProducerProps.put(courseConfig.key_serializer, classOf[StringSerializer])
          kafkaProducerProps.put(courseConfig.value_serializer, classOf[StringSerializer])
          val producer = new KafkaProducer[String, String](kafkaProducerProps)
          val producerData = new util.HashMap[String, Any]
          producerData.put(courseConfig.MESSAGE,courseConfig.CHECK_NOTIFICATION_PREFERENCE_KEY)
          producerData.put(courseConfig.emailWithUserId,emailWithUserId)
          producerData.put(courseConfig.PARAMS,params)
          producerData.put(courseConfig.emailTemplate, courseConfig.NEW_COURSES)
          producerData.put(courseConfig.emailSubject, courseConfig.NEW_COURSES_MAIL_SUBJECT)
          val gson=new Gson()
          val jsonString=gson.toJson(producerData)
          producer.send(new ProducerRecord[String, String](courseConfig.notification_Preference_Kafka_input_topic,  courseConfig.DATA, jsonString))
        }
        offset += limit
        count = response.getHits.getTotalHits.toInt
        logger.info("count at last " + count)
        logger.info("offset in last " + offset)
      }
    } catch {
      case e: Exception => e.printStackTrace()
        logger.info(String.format("Failed during fetching mail %s", e.getMessage()))
        return false
    }
    return true
  }

  def sendNotification(sendTo: java.util.List[String], params: java.util.Map[String, Any], senderMail: String, notificationUrl: String, emailTemplate: String, emailSubject: String): Unit = {
    logger.info("Entering SendNotification")
    new Thread(new Runnable {
      override def run(): Unit = {
        try {
          val notificationTosend: java.util.List[Any] = new java.util.ArrayList[Any](java.util.Arrays.asList(new Notification(courseConfig.EMAIL, courseConfig.MESSAGE, new EmailConfig(sender = senderMail, subject = emailSubject),
            ids = sendTo, new Templates(data = null, id = emailTemplate, params = params))));
          val notificationRequest: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
          notificationRequest.put(courseConfig.REQUEST, new java.util.HashMap[String, java.util.List[Any]]() {
            {
              put(courseConfig.NOTIFICATIONS, notificationTosend)
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

  def updateEmailRecordInTheDatabase(): Unit = {
    try {
      val deleteQuery = QueryBuilder.delete().from(courseConfig.dbSunbirdKeyspace, courseConfig.EMAIL_RECORD_TABLE).where(QueryBuilder.eq(courseConfig.EMAIL_TYPE, courseConfig.NEW_COURSES_EMAIL)).toString
      cassandraUtil.delete(deleteQuery)
      val insertQuery = QueryBuilder.insertInto(courseConfig.dbSunbirdKeyspace, courseConfig.EMAIL_RECORD_TABLE)
        .value(courseConfig.EMAIL_TYPE, courseConfig.NEW_COURSES_EMAIL).value(courseConfig.LAST_SENT_DATE, LocalDate.now().toString).toString
      cassandraUtil.upsert(insertQuery)
    } catch {
      case e: Exception => e.printStackTrace()
        logger.info("Update Email Record in Data base failed " + e.getMessage)
    }
  }
}
