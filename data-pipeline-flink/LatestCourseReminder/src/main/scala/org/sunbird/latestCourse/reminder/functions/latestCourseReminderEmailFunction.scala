package org.sunbird.latestCourse.reminder.functions

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.google.gson.Gson
import org.apache.commons.collections.MapUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import org.sunbird.dp.contentupdater.core.util.RestUtil
import org.sunbird.dp.core.cache.{DataCache, RedisConnect}
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.dp.core.util.CassandraUtil
import org.sunbird.latestCourse.reminder.domain.Event
import org.sunbird.latestCourse.reminder.task.LatestCourseReminderEmailConfig
import org.sunbird.latestCourse.reminder.util.RestApiUtil

import java.time.LocalDate
import java.util
import java.util.concurrent.CompletableFuture
import java.util.{Arrays, Collections, Date, Properties}


class latestCourseReminderEmailFunction(config: LatestCourseReminderEmailConfig,
                                        @transient var cassandraUtil: CassandraUtil = null
                                       )(implicit val mapTypeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](config) {

  case class NewCourseData(id:String,ver:String,ts:String,params:Params,responseCode:String,result: Result)

  case class Params(resmsgid:String,msgid:String,status:String,err:Any,errmsg:Any)

  case class Result(count:Int,content:java.util.List[Content]=null)

  case class Content(trackable:Trackable,instructions:String,identifier:String,purpose:String,channel:String,organisation:java.util.List[String]=null,
                     description:String,creatorLogo:String,mimeType:String,posterImage:String,idealScreenSize:String,version:Int,pkgVersion:Int,
                     objectType:String,learningMode:String,duration:String,license:String,appIcon:String,primaryCategory:String,name:String,lastUpdatedOn:String,contentType:String)

  case class Trackable(enabled:String,autoBatch:String)

  case class CoursesDataMap(courseId:String,courseName:String,thumbnail:String,courseUrl:String,duration:Int,description:String)


  case class Notification(mode: String, deliveryType: String, config: EmailConfig, ids: java.util.List[String], template: Template)

  case class Template(data: String, id: String, params: java.util.Map[String, Any])

  case class EmailConfig(sender: String, subject: String)


  private[this] val logger = LoggerFactory.getLogger(classOf[latestCourseReminderEmailFunction])

  private var dataCache: DataCache = _
  private var contentCache: DataCache = _
  private var restUtil: RestUtil = _
  private var restApiUtil:RestApiUtil=_


  override def metricsList() = List(config.dbUpdateCount, config.dbReadCount,
    config.failedEventCount, config.batchSuccessCount,
    config.skippedEventCount, config.cacheHitCount, config.cacheHitMissCount, config.certIssueEventsCount, config.apiHitFailedCount, config.apiHitSuccessCount, config.ignoredEventsCount, config.recomputeAggEventCount)


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    dataCache = new DataCache(config, new RedisConnect(config.metaRedisHost, config.metaRedisPort, config), config.relationCacheNode, List())
    dataCache.init()
    contentCache = new DataCache(config, new RedisConnect(config.metaRedisHost, config.metaRedisPort, config), config.contentCacheNode, List())
    contentCache.init()
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
    restUtil = new RestUtil()
    restApiUtil = new RestApiUtil()
  }

  override def close(): Unit = {
    super.close()
  }

  /**
   * Method to write the assess event to cassandra table
   *
   * @param event   - Assess Batch Events
   * @param context - Process Context
   */
  override def processElement(event: Event,
                              context: ProcessFunction[Event, Event]#Context,
                              metrics: Metrics): Unit = {
    try {
      initiateLatestCourseAlertEmail()
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        logger.info(s"Getting Incomplete Courses Details Failed with exception ${ex.getMessage}:")
        event.markFailed(ex.getMessage)
        context.output(config.failedEventsOutputTag, event)
        metrics.incCounter(config.failedEventCount)
    }
  }

  def initiateLatestCourseAlertEmail():Unit={
    val newCourseData:NewCourseData=getLatestAddedCourses()
    if(newCourseData!=null && newCourseData.result.content.size()>=config.latest_courses_alert_content_min_limit){
      val coursesDataMapList:util.List[CoursesDataMap]= setCourseMap(newCourseData.result.content)
      if(sendNewCourseEmail(coursesDataMapList)){
        updateEmailRecordInTheDatabase();
      }
    }else{
      logger.info("There are no latest courses or number of latest courses are less than "+config.latest_courses_alert_content_min_limit)
    }
  }

  def getLatestAddedCourses(): NewCourseData ={
    logger.info("Entering getLatestAddedCourses")
    try{
      val lastUpdatedOn=new util.HashMap[String,Any]()
      val maxValue=LocalDate.now()
      lastUpdatedOn.put(config.MIN, calculateMinValue(maxValue))
      lastUpdatedOn.put(config.MAX,maxValue.toString)
      val filters=new util.HashMap[String,Any]()
      filters.put(config.PRIMARY_CATEGORY,Collections.singletonList(config.COURSE))
      filters.put(config.CONTENT_TYPE_SEARCH,Collections.singletonList(config.COURSE))
      filters.put(config.LAST_UPDATED_ON,lastUpdatedOn)
      val sortBy=new util.HashMap[String,Any]()
      sortBy.put(config.LAST_UPDATED_ON,config.DESCENDING_ORDER)
      val searchFields=config.SEARCH_FIELDS
      val request=new util.HashMap[String,Any]()
      request.put(config.FILTERS,filters)
      request.put(config.OFFSET,0)
      request.put(config.LIMIT,1000)
      request.put(config.SORT_BY,sortBy)
      request.put(config.FIELDS, searchFields.split(",", -1))
      val requestBody=new util.HashMap[String,Any]()
      requestBody.put(config.REQUEST,request)
      if(!lastUpdatedOn.get(config.MAX).toString.equalsIgnoreCase(lastUpdatedOn.get(config.MIN).toString)){
        val url:String=config.KM_BASE_HOST+config.content_search
        val obj=fetchResultUsingPost(url,requestBody)
        val gson =new Gson()
        return gson.fromJson(obj,classOf[NewCourseData])
      }
    }catch {
      case e: Exception => e.printStackTrace()
        logger.info(String.format("Error in get and set user email %s", e.getMessage()))
    }
    return null
  }

  def calculateMinValue(maxValue: LocalDate): String ={
    var minValue: String = ""
    val query= QueryBuilder.select().column(config.LAST_SENT_DATE)
      .from(config.sunbird_keyspace, config.EMAIL_RECORD_TABLE)
      .where(QueryBuilder.eq(config.EMAIL_TYPE,config.NEW_COURSES_EMAIL)).allowFiltering().toString
    val emailRecords=cassandraUtil.find(query)
    if(!emailRecords.isEmpty){
      if(!StringUtils.isEmpty(emailRecords.get(0).getString(config.LAST_SENT_DATE))){
        minValue=emailRecords.get(0).getString(config.LAST_SENT_DATE)
      }else{
        minValue=""
      }
    }
    if(StringUtils.isEmpty(minValue)){
      minValue=maxValue.minusDays(config.new_courses_scheduler_time_gap/24).toString
    }
    minValue
  }

  def fetchResultUsingPost(uri:String,requestBody:util.Map[String,Any]):String={
    var response=new String()
    try {
      response= restApiUtil.postRequest(uri, requestBody)
    }catch {
      case  e:Exception=>e.printStackTrace()
        logger.info("Error received: " + e.getMessage())
        response=e.getMessage
    }
    response
  }

  def setCourseMap(courseList: util.List[Content]): util.List[CoursesDataMap] = {
    logger.info("Entering setCourseMap")
    val coursesDataMapList=new util.ArrayList[CoursesDataMap]()
    for(i<- 0 to courseList.size() if i < config.new_courses_email_limit){
      try{
        val Id: String = courseList.get(i).identifier
        if(!StringUtils.isEmpty(courseList.get(i).identifier) && !StringUtils.isEmpty(courseList.get(i).name) && !StringUtils.isEmpty(courseList.get(i).posterImage) && !StringUtils.isEmpty(courseList.get(i).duration)){
          val courseName = courseList.get(i).name.toLowerCase.capitalize
          val  thumbnail = courseList.get(i).posterImage
          val courseUrl = config.COURSE_URL+Id
          val description = courseList.get(i).description
          val duration = courseList.get(i).duration.toInt
          val coursesDataMap=new CoursesDataMap(Id,courseName,thumbnail,courseUrl,duration,description)
          coursesDataMapList.add(coursesDataMap)
        }
      }catch {
        case e: Exception => e.printStackTrace()
          logger.info("Error while set course : " + e.getMessage())
      }
    }
    coursesDataMapList
  }


  def sendNewCourseEmail(coursesDataMapList: util.List[CoursesDataMap]): Boolean = {
    try{
      logger.info("Entering new courses email")
      val params=new util.HashMap[String,Any]()
      params.put(config.NO_OF_COURSES,coursesDataMapList.size())
      for(i<- 0 to coursesDataMapList.size() if i < config.new_courses_email_limit){
        val j:Int=i+1;
        params.put(config.COURSE_KEYWORD + j,true)
        params.put(config.COURSE_KEYWORD + j +config._URL,coursesDataMapList.get(i).courseUrl)
        params.put(config.COURSE_KEYWORD+ j +config.THUMBNAIL,coursesDataMapList.get(i).thumbnail)
        params.put(config.COURSE_KEYWORD+ j +config._NAME,coursesDataMapList.get(i).courseName)
        params.put(config.COURSE_KEYWORD+ j +config._DURATION,convertSecondsToHrsAndMinutes(coursesDataMapList.get(i).duration))
        params.put(config.COURSE_KEYWORD+ j +config._DESCRIPTION,coursesDataMapList.get(i).description)
      }
      val isEmailSentToConfigMailIds:Boolean= sendEmailsToConfigBasedMailIds(params)
      var isEmailSentToESMailIds: Boolean = false
      if(config.latest_courses_alert_send_to_all_user){
        val query= QueryBuilder.select().column(config.EMAIL)
          .from(config.sunbird_keyspace, config.EXCLUDE_USER_EMAILS)
          .allowFiltering().toString
        val excludeEmails = cassandraUtil.find(query)
        val excludeEmailsList: java.util.List[Any] = new java.util.ArrayList[Any]()
        for (i <- 0 to excludeEmails.size() - 1) {
          excludeEmailsList.add(excludeEmails.get(i))
        }
        isEmailSentToESMailIds= fetchEmailIdsFromUserES(excludeEmailsList,params)
      }
      return isEmailSentToConfigMailIds
    }catch {
      case e: Exception => e.printStackTrace()
        logger.error("Error while sending New Course Email : " + e.getMessage())
    }
    return false
  }

  def convertSecondsToHrsAndMinutes(seconds:Int):String={
    var time: String = ""
    if(seconds>60){
      val min:Int=(seconds/60)%60
      val hours:Int=(seconds/60)/60
      var minutes = new String()
      if(min<10){
        minutes="0"+min
      }else{
        minutes=min.toString
      }
      var strHours = new String()
      if (hours < 10) {
        strHours = "0" + hours
      } else {
        strHours = hours.toString
      }
      if(min>0 && hours>0){
        time=strHours+"h "+minutes+"m"
      }else if(min==0 && hours>0){
        time=strHours+"h"
      }else if(min > 0){
        time=minutes+"m"
      }
    }
    return time
  }

  def sendEmailsToConfigBasedMailIds(params: util.HashMap[String, Any]): Boolean = {
    logger.info("Entering sendEmailsToConfigBasedMailIds")
    try{
      val mailList:util.List[String]=new util.ArrayList[String]()
      val mails:util.List[String]=new util.ArrayList[String]()
      val mail=config.MAIL_LIST.split(",",-1)
      mail.foreach(i=>mails.add(i))
      mails.forEach(m=>mailList.add(m))
      //sendNotification(mailList,params,config.SENDER_MAIL,config.notification_service_host+config.notification_event_endpoint,config.NEW_COURSES,config.NEW_COURSES_MAIL_SUBJECT)
      return true
    }catch {
      case e: Exception => e.printStackTrace()
        logger.info(String.format("Failed during sending mail %s", e.getMessage()))
    }
    return true
  }

  def fetchEmailIdsFromUserES(excludeEmailsList: util.List[Any],params:util.Map[String,Any]): Boolean = {
    logger.info("Entering fetchEmailIdsFromUserES")
    try{
      var count: Int = 1
      val limit: Int = 45
      val filters=new util.HashMap[String,Any]()
      filters.put(config.STATUS,1)
      filters.put(config.IS_DELETED,false)
      val searchFields=config.EMAIL_SEARCH_FIELDS
      val request=new util.HashMap[String,Any]()
      request.put(config.FILTERS,filters)
      request.put(config.LIMIT,limit)
      request.put(config.FIELDS, searchFields.split(",", -1))
      val requestBody=new util.HashMap[String,Any]()
      requestBody.put(config.REQUEST,request)
      val headersValue=new util.HashMap[String,Any]()
      headersValue.put(config.CONTENT_TYPE,config.APPLICATION_JSON)
      headersValue.put(config.AUTHORIZATION,config.SB_API_KEY)
      var response=new util.HashMap[String,Any]()
      val url=config.SB_SERVICE_URL+config.SUNBIRD_USER_SEARCH_ENDPOINT
      var offset:Int=0
      while (offset<count){
        val emails=new util.ArrayList[String]()
        request.put(config.OFFSET,offset)
        val responseString=restApiUtil.postRequestForSearchUser(url,requestBody,headersValue)
        val gson =new Gson()
        response=gson.fromJson(responseString,classOf[util.HashMap[String,Any]])
        if(response!=null && config.OK.equalsIgnoreCase(response.get(config.RESPONSE_CODE).toString)) {
          val map: util.Map[String, Any] = response.get(config.RESULT).asInstanceOf[util.Map[String, Any]]
          if (map.get(config.RESPONSE) != null) {
            val responseObj: util.Map[String, Any] = map.get(config.RESPONSE).asInstanceOf[util.Map[String, Any]]
            val contents: util.List[util.Map[String, Any]] = responseObj.get(config.CONTENT).asInstanceOf[util.List[util.Map[String, Any]]]
            if (offset == 0) {
              val c = responseObj.get(config.COUNT).asInstanceOf[Double]
              count = c.toInt
            }
            for (i <- 0 to contents.size()-1) {
              val content = new util.HashMap[String, Any]
              content.putAll(contents.get(i))
              if (content.containsKey(config.PROFILE_DETAILS)) {
                val profileDetails: util.Map[String, Any] = content.get(config.PROFILE_DETAILS).asInstanceOf[util.Map[String, Any]]
                if (profileDetails.containsKey(config.PERSONAL_DETAILS)) {
                  val personalDetails: util.Map[String, Any] = profileDetails.get(config.PERSONAL_DETAILS).asInstanceOf[util.Map[String, Any]]
                  if (MapUtils.isNotEmpty(personalDetails)) {
                    val email = personalDetails.get(config.PRIMARY_EMAIL).toString
                    //TODO-If condition need to check
                    if(StringUtils.isNotBlank(email) && !excludeEmailsList.contains(email)){
                      if (config.MAIL_LIST != null && !config.MAIL_LIST.contains(email)) {
                        emails.add(email)
                      } else {
                        logger.info("Invalid Email :" + personalDetails.get(config.PRIMARY_EMAIL))
                      }
                    }
                  }
                }
              }
            }
          }
        }
        val kafkaProducerProps=new Properties()
        kafkaProducerProps.put("bootstrap.servers",config.BOOTSTRAP_SERVER_CONFIG)
        kafkaProducerProps.put("key.serializer",classOf[StringSerializer].getName)
        kafkaProducerProps.put("value.serializer",classOf[StringSerializer].getName)
        val producer=new KafkaProducer[String,String](kafkaProducerProps)
        val producerData=new util.HashMap[String,Any]
        producerData.put(config.EMAILS,emails)
        producerData.put(config.PARAMS,params)
        producerData.put(config.emailTemplate,config.NEW_COURSES)
        producerData.put(config.emailSubject,config.NEW_COURSES_MAIL_SUBJECT)
        producer.send(new ProducerRecord[String,String](config.kafkaOutPutStreamTopic,config.DATA,producerData.toString))
        /*producer.flush()
        producer.close()*/
        /* CompletableFuture.runAsync(()=>{
           //sendNotification(emails,params,config.SENDER_MAIL,config.notification_service_host+config.notification_event_endpoint,config.NEW_COURSES,config.NEW_COURSES_MAIL_SUBJECT)
         })*/
        offset += limit
        logger.info("offset in last "+offset)
      }
    }catch {
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

  def updateEmailRecordInTheDatabase(): Unit = {
    try {
      val deleteQuery = QueryBuilder.delete().from(config.sunbird_keyspace, config.EMAIL_RECORD_TABLE).where(QueryBuilder.eq(config.EMAIL_TYPE, config.NEW_COURSES_EMAIL)).toString
      cassandraUtil.delete(deleteQuery)
      val insertQuery = QueryBuilder.insertInto(config.sunbird_keyspace, config.EMAIL_RECORD_TABLE)
        .value(config.EMAIL_TYPE,config.NEW_COURSES_EMAIL).value(config.LAST_SENT_DATE, LocalDate.now().toString).toString
      cassandraUtil.upsert(insertQuery)
    } catch {
      case e: Exception => e.printStackTrace()
        logger.info("Update Email Record in Data base failed " + e.getMessage)
    }
  }
}