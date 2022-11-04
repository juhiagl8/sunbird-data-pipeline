package org.sunbird.latestCourse.reminder.functions

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.ScalaObjectMapper
import org.elasticsearch.index.query._
import com.google.gson.Gson
import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.velocity.{Template, VelocityContext}
import org.apache.velocity.app.{Velocity, VelocityEngine}
import org.apache.velocity.runtime.RuntimeConstants
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.search.{SearchHit, SearchHits}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.slf4j.LoggerFactory
import org.sunbird.dp.contentupdater.core.util.RestUtil
import org.sunbird.dp.core.cache.{DataCache, RedisConnect}
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.dp.core.util.CassandraUtil
import org.sunbird.latestCourse.reminder.domain.Event
import org.sunbird.latestCourse.reminder.task.LatestCourseReminderEmailConfig
import org.sunbird.latestCourse.reminder.util.{IndexService, ReadValue, RestApiUtil}

import java.io.StringWriter
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.LocalDate
import java.util
import java.util.Map.Entry
import java.util.concurrent.CompletableFuture
import java.util.{Arrays, Collections, Date, Properties, UUID}


class latestCourseReminderEmailFunction(courseConfig: LatestCourseReminderEmailConfig,
                                        @transient var cassandraUtil: CassandraUtil = null
                                       )(implicit val mapTypeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](courseConfig) {

  case class NewCourseData(id:String,ver:String,ts:String,params:Params,responseCode:String,result: Result)

  case class Params(resmsgid:String,msgid:String,status:String,err:Any,errmsg:Any)

  case class Result(count:Int,content:java.util.List[Content]=null)

  case class Content(trackable:Trackable,instructions:String,identifier:String,purpose:String,channel:String,organisation:java.util.List[String]=null,
                     description:String,creatorLogo:String,mimeType:String,posterImage:String,idealScreenSize:String,version:Int,pkgVersion:Int,
                     objectType:String,learningMode:String,duration:String,license:String,appIcon:String,primaryCategory:String,name:String,lastUpdatedOn:String,contentType:String)

  case class Trackable(enabled:String,autoBatch:String)

  case class CoursesDataMap(courseId:String,courseName:String,thumbnail:String,courseUrl:String,duration:Int,description:String)


  case class Notification(mode: String, deliveryType: String, config: EmailConfig, ids: java.util.List[String], template: Templates)

  case class Templates(data: String, id: String, params: java.util.Map[String, Any])

  case class EmailConfig(sender: String, subject: String)


  private[this] val logger = LoggerFactory.getLogger(classOf[latestCourseReminderEmailFunction])

  private var dataCache: DataCache = _
  private var contentCache: DataCache = _
  private var restUtil: RestUtil = _
  private var restApiUtil:RestApiUtil=_


  override def metricsList() = List(courseConfig.dbUpdateCount, courseConfig.dbReadCount,
    courseConfig.failedEventCount, courseConfig.batchSuccessCount,
    courseConfig.skippedEventCount, courseConfig.cacheHitCount, courseConfig.cacheHitMissCount, courseConfig.certIssueEventsCount, courseConfig.apiHitFailedCount, courseConfig.apiHitSuccessCount, courseConfig.ignoredEventsCount, courseConfig.recomputeAggEventCount)


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    dataCache = new DataCache(courseConfig, new RedisConnect(courseConfig.metaRedisHost, courseConfig.metaRedisPort, courseConfig), courseConfig.relationCacheNode, List())
    dataCache.init()
    contentCache = new DataCache(courseConfig, new RedisConnect(courseConfig.metaRedisHost, courseConfig.metaRedisPort, courseConfig), courseConfig.contentCacheNode, List())
    contentCache.init()
    cassandraUtil = new CassandraUtil(courseConfig.dbHost, courseConfig.dbPort)
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
        context.output(courseConfig.failedEventsOutputTag, event)
        metrics.incCounter(courseConfig.failedEventCount)
    }
  }

  def initiateLatestCourseAlertEmail():Unit={
    val newCourseData:NewCourseData=getLatestAddedCourses()
    if(newCourseData!=null && newCourseData.result.content.size()>=courseConfig.latest_courses_alert_content_min_limit){
      val coursesDataMapList:util.List[CoursesDataMap]= setCourseMap(newCourseData.result.content)
      if(sendNewCourseEmail(coursesDataMapList)){
        //updateEmailRecordInTheDatabase();
      }
    }else{
      logger.info("There are no latest courses or number of latest courses are less than "+courseConfig.latest_courses_alert_content_min_limit)
    }
  }

  def getLatestAddedCourses(): NewCourseData ={
    logger.info("Entering getLatestAddedCourses")
    try{
      val lastUpdatedOn=new util.HashMap[String,Any]()
      val maxValue=LocalDate.now()
      lastUpdatedOn.put(courseConfig.MIN, calculateMinValue(maxValue))
      lastUpdatedOn.put(courseConfig.MAX,maxValue.toString)
      val filters=new util.HashMap[String,Any]()
      filters.put(courseConfig.PRIMARY_CATEGORY,Collections.singletonList(courseConfig.COURSE))
      filters.put(courseConfig.CONTENT_TYPE_SEARCH,Collections.singletonList(courseConfig.COURSE))
      filters.put(courseConfig.LAST_UPDATED_ON,lastUpdatedOn)
      val sortBy=new util.HashMap[String,Any]()
      sortBy.put(courseConfig.LAST_UPDATED_ON,courseConfig.DESCENDING_ORDER)
      val searchFields=courseConfig.SEARCH_FIELDS
      val request=new util.HashMap[String,Any]()
      request.put(courseConfig.FILTERS,filters)
      request.put(courseConfig.OFFSET,0)
      request.put(courseConfig.LIMIT,1000)
      request.put(courseConfig.SORT_BY,sortBy)
      request.put(courseConfig.FIELDS, searchFields.split(",", -1))
      val requestBody=new util.HashMap[String,Any]()
      requestBody.put(courseConfig.REQUEST,request)
      if(!lastUpdatedOn.get(courseConfig.MAX).toString.equalsIgnoreCase(lastUpdatedOn.get(courseConfig.MIN).toString)){
        val url:String=courseConfig.KM_BASE_HOST+courseConfig.content_search
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
    val query= QueryBuilder.select().column(courseConfig.LAST_SENT_DATE)
      .from(courseConfig.sunbird_keyspace, courseConfig.EMAIL_RECORD_TABLE)
      .where(QueryBuilder.eq(courseConfig.EMAIL_TYPE,courseConfig.NEW_COURSES_EMAIL)).allowFiltering().toString
    val emailRecords=cassandraUtil.find(query)
    if(!emailRecords.isEmpty){
      if(!StringUtils.isEmpty(emailRecords.get(0).getString(courseConfig.LAST_SENT_DATE))){
        minValue=emailRecords.get(0).getString(courseConfig.LAST_SENT_DATE)
      }else{
        minValue=""
      }
    }
    if(StringUtils.isEmpty(minValue)){
      minValue=maxValue.minusDays(courseConfig.new_courses_scheduler_time_gap/24).toString
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
    for(i<- 0 to courseList.size()-1 if i < courseConfig.new_courses_email_limit){
      try{
        val Id: String = courseList.get(i).identifier
        if(!StringUtils.isEmpty(courseList.get(i).identifier) && !StringUtils.isEmpty(courseList.get(i).name) && !StringUtils.isEmpty(courseList.get(i).posterImage) && !StringUtils.isEmpty(courseList.get(i).duration)){
          val courseName = courseList.get(i).name.toLowerCase.capitalize
          val  thumbnail = courseList.get(i).posterImage
          val courseUrl = courseConfig.COURSE_URL+Id
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
      params.put(courseConfig.NO_OF_COURSES,coursesDataMapList.size())
      for(i<- 0 to coursesDataMapList.size()-1 if i < courseConfig.new_courses_email_limit){
        val j:Int=i+1;
        params.put(courseConfig.COURSE_KEYWORD + j,true)
        params.put(courseConfig.COURSE_KEYWORD + j +courseConfig._URL,coursesDataMapList.get(i).courseUrl)
        params.put(courseConfig.COURSE_KEYWORD+ j +courseConfig.THUMBNAIL,coursesDataMapList.get(i).thumbnail)
        params.put(courseConfig.COURSE_KEYWORD+ j +courseConfig._NAME,coursesDataMapList.get(i).courseName)
        params.put(courseConfig.COURSE_KEYWORD+ j +courseConfig._DURATION,convertSecondsToHrsAndMinutes(coursesDataMapList.get(i).duration))
        params.put(courseConfig.COURSE_KEYWORD+ j +courseConfig._DESCRIPTION,coursesDataMapList.get(i).description)
      }
      val isEmailSentToConfigMailIds:Boolean= sendEmailsToConfigBasedMailIds(params)
      var isEmailSentToESMailIds: Boolean = false
      if(courseConfig.latest_courses_alert_send_to_all_user){
        val query= QueryBuilder.select().column(courseConfig.EMAIL)
          .from(courseConfig.sunbird_keyspace, courseConfig.EXCLUDE_USER_EMAILS)
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
      val mail=courseConfig.MAIL_LIST.split(",",-1)
      mail.foreach(i=>mails.add(i))
      mails.forEach(m=>mailList.add(m))
      //sendNotification(mailList,params,courseConfig.SENDER_MAIL,courseConfig.notification_service_host+courseConfig.notification_event_endpoint,courseConfig.NEW_COURSES,courseConfig.NEW_COURSES_MAIL_SUBJECT)
      return true
    }catch {
      case e: Exception => e.printStackTrace()
        logger.info(String.format("Failed during sending mail %s", e.getMessage()))
    }
    return true
  }

  //This method talks to sunbird-lms-service to collect the user details like primary email

  /* def fetchEmailIdsFromUserES(excludeEmailsList: util.List[Any],params:util.Map[String,Any]): Boolean = {
     logger.info("Entering fetchEmailIdsFromUserES")
     try{
       var count: Int = 1
       val limit: Int = 45
       val filters=new util.HashMap[String,Any]()
       filters.put(courseConfig.STATUS,1)
       filters.put(courseConfig.IS_DELETED,false)
       val searchFields=courseConfig.EMAIL_SEARCH_FIELDS
       val request=new util.HashMap[String,Any]()
       request.put(courseConfig.FILTERS,filters)
       request.put(courseConfig.LIMIT,limit)
       request.put(courseConfig.FIELDS, searchFields.split(",", -1))
       val requestBody=new util.HashMap[String,Any]()
       requestBody.put(courseConfig.REQUEST,request)
       val headersValue=new util.HashMap[String,Any]()
       headersValue.put(courseConfig.CONTENT_TYPE,courseConfig.APPLICATION_JSON)
       headersValue.put(courseConfig.AUTHORIZATION,courseConfig.SB_API_KEY)
       var response=new util.HashMap[String,Any]()
       val url=courseConfig.SB_SERVICE_URL+courseConfig.SUNBIRD_USER_SEARCH_ENDPOINT
       var offset:Int=0
       while (offset<count){
         val emails=new util.ArrayList[String]()
         request.put(courseConfig.OFFSET,offset)
         val responseString=restApiUtil.postRequestForSearchUser(url,requestBody,headersValue)
         val gson =new Gson()
         response=gson.fromJson(responseString,classOf[util.HashMap[String,Any]])
         if(response!=null && courseConfig.OK.equalsIgnoreCase(response.get(courseConfig.RESPONSE_CODE).toString)) {
           val map: util.Map[String, Any] = response.get(courseConfig.RESULT).asInstanceOf[util.Map[String, Any]]
           if (map.get(courseConfig.RESPONSE) != null) {
             val responseObj: util.Map[String, Any] = map.get(courseConfig.RESPONSE).asInstanceOf[util.Map[String, Any]]
             val contents: util.List[util.Map[String, Any]] = responseObj.get(courseConfig.CONTENT).asInstanceOf[util.List[util.Map[String, Any]]]
             if (offset == 0) {
               val c = responseObj.get(courseConfig.COUNT).asInstanceOf[Double]
               count = c.toInt
             }
             for (i <- 0 to contents.size()-1) {
               val content = new util.HashMap[String, Any]
               content.putAll(contents.get(i))
               if (content.containsKey(courseConfig.PROFILE_DETAILS)) {
                 val profileDetails: util.Map[String, Any] = content.get(courseConfig.PROFILE_DETAILS).asInstanceOf[util.Map[String, Any]]
                 if (profileDetails.containsKey(courseConfig.PERSONAL_DETAILS)) {
                   val personalDetails: util.Map[String, Any] = profileDetails.get(courseConfig.PERSONAL_DETAILS).asInstanceOf[util.Map[String, Any]]
                   if (MapUtils.isNotEmpty(personalDetails)) {
                     val email = personalDetails.get(courseConfig.PRIMARY_EMAIL).toString
                     //TODO-If condition need to check
                     if(StringUtils.isNotBlank(email) && !excludeEmailsList.contains(email)){
                       if (courseConfig.MAIL_LIST != null && !courseConfig.MAIL_LIST.contains(email)) {
                         emails.add(email)
                       } else {
                         logger.info("Invalid Email :" + personalDetails.get(courseConfig.PRIMARY_EMAIL))
                       }
                     }
                   }
                 }
               }
             }
           }
         }
         val kafkaProducerProps=new Properties()
         kafkaProducerProps.put("bootstrap.servers",courseConfig.BOOTSTRAP_SERVER_CONFIG)
         kafkaProducerProps.put("key.serializer",classOf[StringSerializer].getName)
         kafkaProducerProps.put("value.serializer",classOf[StringSerializer].getName)
         val producer=new KafkaProducer[String,String](kafkaProducerProps)
         val producerData=new util.HashMap[String,Any]
         producerData.put(courseConfig.EMAILS,emails)
         producerData.put(courseConfig.PARAMS,params)
         producerData.put(courseConfig.emailTemplate,courseConfig.NEW_COURSES)
         producerData.put(courseConfig.emailSubject,courseConfig.NEW_COURSES_MAIL_SUBJECT)
         producer.send(new ProducerRecord[String,String](courseConfig.kafkaOutPutStreamTopic,courseConfig.DATA,producerData.toString))
         /*producer.flush()
         producer.close()*/
         /* CompletableFuture.runAsync(()=>{
            //sendNotification(emails,params,courseConfig.SENDER_MAIL,courseConfig.notification_service_host+courseConfig.notification_event_endpoint,courseConfig.NEW_COURSES,courseConfig.NEW_COURSES_MAIL_SUBJECT)
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
   }*/

  def fetchEmailIdsFromUserES(excludeEmailsList: util.List[Any], params: util.Map[String, Any]): Boolean = {
    val resultArray=new util.ArrayList[util.HashMap[String,Any]]
    var result=new util.HashMap[String,Any]()
    logger.info("Entering fetchEmailIdsFromUserES")
    try {
      var count: Int = 1
      val limit: Int = 45
      var offset: Int = 0
      var response=new SearchResponse()
      while (offset<count){
        val emails = new util.ArrayList[String]()
        val query: BoolQueryBuilder = QueryBuilders.boolQuery()
        val finalQuery: BoolQueryBuilder = QueryBuilders.boolQuery()
        finalQuery.must(QueryBuilders.matchQuery(courseConfig.STATUS, 1))
          .must(QueryBuilders.matchQuery(courseConfig.IS_DELETED, false)).must(query)
        val sourceBuilder = new SearchSourceBuilder().query(finalQuery)
        sourceBuilder.fetchSource(courseConfig.fields, new String())
        sourceBuilder.from(offset)
        sourceBuilder.size(45)
        val index = new IndexService()
        response= index.getEsResult(courseConfig.sb_es_user_profile_index, courseConfig.es_profile_index_type, sourceBuilder, true)
        response.getHits.forEach(hitDetails=>{
          result = hitDetails.getSourceAsMap().asInstanceOf[util.HashMap[String, Any]]
          if (result.containsKey(courseConfig.PROFILE_DETAILS)) {
            val userId=result.get(courseConfig.userId)
            val profileDetails: util.HashMap[String, Any] = result.get(courseConfig.PROFILE_DETAILS).asInstanceOf[util.HashMap[String, Any]]
            if (profileDetails.containsKey(courseConfig.PERSONAL_DETAILS)) {
              val personalDetails: util.HashMap[String, Any] = profileDetails.get(courseConfig.PERSONAL_DETAILS).asInstanceOf[util.HashMap[String, Any]]
              if (MapUtils.isNotEmpty(personalDetails)) {
                val email: String = personalDetails.get(courseConfig.PRIMARY_EMAIL).asInstanceOf[String]
                if (StringUtils.isNotBlank(email) && !excludeEmailsList.contains(email)) {
                  if (courseConfig.MAIL_LIST != null && !courseConfig.MAIL_LIST.contains(email)) {
                    emails.add(email)
                  } else {
                    logger.info("Invalid Email :" + email)
                  }
                }
              }
            }
          }
        })
        val Actor=new util.HashMap[String,String]()
        Actor.put(courseConfig.ID,courseConfig.BROAD_CAST_TOPIC_NOTIFICATION_MESSAGE)
        Actor.put(courseConfig.TYPE,courseConfig.ACTOR_TYPE_VALUE)
        val eid=courseConfig.EID_VALUE
        val edata=new util.HashMap[String,Any]()
        edata.put(courseConfig.ACTION,courseConfig.BROAD_CAST_TOPIC_NOTIFICATION_KEY)
        edata.put(courseConfig.iteration,1)
        val request=new util.HashMap[String,Any]()
        val config=new util.HashMap[String,String]()
        config.put(courseConfig.SENDER,courseConfig.SENDER_MAIL)
        config.put(courseConfig.TOPIC,null)
        config.put(courseConfig.OTP,null)
        config.put(courseConfig.SUBJECT,courseConfig.NEW_COURSES_MAIL_SUBJECT)
        //var template=Templates(data = null, id = courseConfig.NEW_COURSES_EMAIL, params = params)

        val templates=new util.HashMap[String,Any]()
        templates.put(courseConfig.DATA,null)
        templates.put(courseConfig.ID,courseConfig.NEW_COURSES_EMAIL)
        templates.put(courseConfig.PARAMS,params)
        logger.info("templates "+templates)

        val notification = new util.HashMap[String, Any]()
        notification.put(courseConfig.rawData, null)
        notification.put(courseConfig.CONFIG, config)
        notification.put(courseConfig.DELIVERY_TYPE, courseConfig.MESSAGE)
        notification.put(courseConfig.DELIVERY_MODE, courseConfig.EMAIL)
        notification.put(courseConfig.TEMPLATE,templates)
        notification.put(courseConfig.IDS,emails)

        logger.info("notification "+notification)

        request.put(courseConfig.NOTIFICATION,notification)
        edata.put(courseConfig.REQUEST,request)
        val trace=new util.HashMap[String,Any]()
        trace.put(courseConfig.X_REQUEST_ID,null)
        trace.put(courseConfig.X_TRACE_ENABLED,false)
        val pdata=new util.HashMap[String,Any]()
        pdata.put(courseConfig.VER,"1.0")
        pdata.put(courseConfig.ID,"org.sunbird.platform")
        val context=new util.HashMap[String,Any]()
        context.put(courseConfig.PDATA,pdata)
        val ets=System.currentTimeMillis()
        val mid = courseConfig.PRODUCER_ID + "." + ets + "." + UUID.randomUUID();
        val objectsDetails=new util.HashMap[String,Any]()
        objectsDetails.put(courseConfig.ID,getRequestHashed(request, context))
        objectsDetails.put(courseConfig.TYPE,courseConfig.TYPE_VALUE)

        var message=new String()
        logger.info("failed conversion of null")
        if(notification.get(courseConfig.TEMPLATE)!=null && templates.get(courseConfig.DATA)!=null){
          logger.info("Entering If block")
          message= getDataMessage(templates.get(courseConfig.DATA).toString,templates.get(courseConfig.PARAMS).asInstanceOf[util.Map[String,Any]],context)
          templates.put(courseConfig.DATA,message)
        }else if (templates.get(courseConfig.ID) != null && notification.get(courseConfig.TEMPLATE)!=null) {
          logger.info("Entering Else If block")
          val dataString = createNotificationBody(templates, context)
          templates.put(courseConfig.DATA,dataString)
        }

        if(CollectionUtils.isNotEmpty(emails)){
          val kafkaProducerProps = new Properties()
          kafkaProducerProps.put("bootstrap.servers", courseConfig.BOOTSTRAP_SERVER_CONFIG)
          kafkaProducerProps.put("key.serializer", classOf[StringSerializer].getName)
          kafkaProducerProps.put("value.serializer", classOf[StringSerializer].getName)
          val producer = new KafkaProducer[String, String](kafkaProducerProps)
          val producerData = new util.HashMap[String, Any]
          producerData.put(courseConfig.ACTOR,Actor)
          producerData.put(courseConfig.EDATA,edata)
          producerData.put(courseConfig.EID,eid)
          producerData.put(courseConfig.TRACE,trace)
          producerData.put(courseConfig.CONTEXT,context)
          producerData.put(courseConfig.MID,mid)
          producerData.put(courseConfig.OBJECT,objectsDetails)
          val mapper:ObjectMapper= new ObjectMapper() with ScalaObjectMapper
          mapper.setVisibility(PropertyAccessor.ALL, Visibility.ANY)
          val jsonString=mapper.writeValueAsString(producerData)
          logger.info("json String "+jsonString)
          //producer.send(new ProducerRecord[String, String](courseConfig.kafkaOutPutStreamTopic, courseConfig.DATA, jsonString))
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

  def getRequestHashed(request:util.HashMap[String,Any],context:util.HashMap[String,Any]):String={
    var value = new String()
    try{
      val mapper:ObjectMapper=new ObjectMapper()
      val mapValue=mapper.writeValueAsString(request)
      val md=MessageDigest.getInstance("SHA-256")
      md.update(mapValue.getBytes(StandardCharsets.UTF_8))
      val byteData = md.digest
      val sb=new StringBuilder()
      for(i<-0 to byteData.length-1){
        sb.append(Integer.toString((byteData(i) & 0xff) + 0x100, 16).substring(1))
      }
      value=sb.toString()
    }catch {
      case e: Exception => e.printStackTrace()
        logger.error("Error while encrypting", e);
        value= ""
    }
    value
  }


  def createNotificationBody(template: util.HashMap[String,Any], context: util.HashMap[String, Any]):String = {
    logger.info("Entering createNotificationBody")
    readVm(template.get(courseConfig.ID).toString, template.get(courseConfig.PARAMS).asInstanceOf[util.HashMap[String,Any]], context);
  }

  def readVm(templateName: String, node: util.Map[String, Any], reqContext: util.HashMap[String, Any]): String = {
    logger.info("Entering readVm")
    val engine:VelocityEngine=new VelocityEngine()
    val context: VelocityContext=getContextObj(node)
    val props: Properties=new Properties()
    //props.setProperty("resource.loader","class")
   /* props.setProperty("file.resource.loader.path", "org/sunbird/latestCourse/reminder/template");
    props.setProperty("class.resource.loader.class","org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader")*/
   /* engine.setProperty(RuntimeConstants.RESOURCE_LOADER, "classpath");
    engine.setProperty("classpath.resource.loader.class", classOf[ClasspathResourceLoader].getName());*/
    props.setProperty("resource.loader", "class");
    props.setProperty("class.resource.loader.class", "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
    var writer: StringWriter=null
    var body=new String()
    val TEMPLATE_SUFFIX=".vm"
    try{
      engine.init(props)
      val templates:Template=engine.getTemplate("templates/" +templateName+TEMPLATE_SUFFIX)
      writer=new StringWriter()
      templates.merge(context,writer)
      body=writer.toString
    }catch {
      case e: Exception => e.printStackTrace()
        logger.error(reqContext+" Failed to load velocity template =" + templateName)
    }finally {
      if (writer!=null){
        try{
          writer.close()
        }catch {
          case e:Exception=>e.printStackTrace()
            logger.error("Failed to closed writer object ="+e.getMessage)
        }
      }
    }
    logger.info("Body from readVM "+body)
    body
  }

  def getContextObj(node: util.Map[String, Any]): VelocityContext = {
    logger.info("Entering getContextObject")
    val util=new ReadValue()
    var context : VelocityContext=null
    if(node!=null){
      context=new VelocityContext(node)
    }else{
      context=new VelocityContext()
    }
    if(!context.containsKey(courseConfig.FROM_EMAIL)){
      context.put(courseConfig.FROM_EMAIL,courseConfig.sunbird_mail_server_from_email)
    }
    if(!context.containsKey(courseConfig.orgImageUrl)){
      context.put(courseConfig.orgImageUrl,courseConfig.sunbird_mail_server_from_email)
    }
    logger.info("context object "+context)
    context
  }

  def getDataMessage(message: String, node: util.Map[String, Any], reqContext: util.HashMap[String, Any]): String = {
    logger.info("Entering getDataMessage")
    val context:VelocityContext=new VelocityContext()
    if(node!=null){
      val itr:util.Iterator[Entry[String,Any]]=node.entrySet().iterator()
      while (itr.hasNext){
        val entry:Entry[String,Any]=itr.next()
        if(null!=entry.getValue()){
          context.put(entry.getKey,entry.getValue)
        }
      }
    }
    var writer :StringWriter=null
    try{
      Velocity.init()
      writer=new StringWriter()
      Velocity.evaluate(context,writer,"SimpleVelocity",message)
    }catch {
      case e:Exception=>e.printStackTrace()
        logger.error("NotificationRouter:getMessage : Exception occurred with message ="+e.getMessage)
    }
    logger.info("Writer Object "+writer.toString)
    writer.toString
  }


  // Added Notification preference not tested yet

  /* def fetchEmailIdsFromUserES(excludeEmailsList: util.List[Any], params: util.Map[String, Any]): Boolean = {
     val resultArray = new util.ArrayList[util.HashMap[String, Any]]
     var result = new util.HashMap[String, Any]()
     logger.info("Entering fetchEmailIdsFromUserES")
     try {
       var count: Int = 1
       val limit: Int = 45
       var offset: Int = 0
       var response = new SearchResponse()
       while (offset < count) {
         val emails = new util.ArrayList[String]()
         val query: BoolQueryBuilder = QueryBuilders.boolQuery()
         val finalQuery: BoolQueryBuilder = QueryBuilders.boolQuery()
         finalQuery.must(QueryBuilders.matchQuery(courseConfig.STATUS, 1))
           .must(QueryBuilders.matchQuery(courseConfig.IS_DELETED, false)).must(query)
         val sourceBuilder = new SearchSourceBuilder().query(finalQuery)
         sourceBuilder.fetchSource(courseConfig.fields, new String())
         sourceBuilder.from(offset)
         sourceBuilder.size(45)
         val index = new IndexService()
         response = index.getEsResult(courseConfig.sb_es_user_profile_index, courseConfig.es_profile_index_type, sourceBuilder, true)
         logger.info("response " + response)
         response.getHits.forEach(hitDetails => {
           result = hitDetails.getSourceAsMap().asInstanceOf[util.HashMap[String, Any]]
           logger.info("result " + hitDetails)
           if (result.containsKey(courseConfig.PROFILE_DETAILS)) {
             val userId = result.get(courseConfig.userId)
             val profileDetails: util.HashMap[String, Any] = result.get(courseConfig.PROFILE_DETAILS).asInstanceOf[util.HashMap[String, Any]]
             if (profileDetails.containsKey(courseConfig.PERSONAL_DETAILS)) {
               val personalDetails: util.HashMap[String, Any] = profileDetails.get(courseConfig.PERSONAL_DETAILS).asInstanceOf[util.HashMap[String, Any]]
               if (MapUtils.isNotEmpty(personalDetails)) {
                 val email: String = personalDetails.get(courseConfig.PRIMARY_EMAIL).asInstanceOf[String]
                 logger.info("email " + email)
                 val query2: BoolQueryBuilder = QueryBuilders.boolQuery()
                 query2.must(QueryBuilders.matchQuery(courseConfig.userId, userId)).must(query)
                 val sourceBuilder = new SearchSourceBuilder().query(query2)
                 sourceBuilder.fetchSource(courseConfig.PREFERENCELIST, new String())
                 val notificationPreferenceResponse=index.getEsResult(courseConfig.sb_es_user_notification_preference, courseConfig.es_preference_index_type, sourceBuilder, true)
                 var preferenceResult = new util.HashMap[String, Any]()
                 var latestCourseAlert=new Boolean()
                 notificationPreferenceResponse.getHits.forEach(preferencehitDetails=>{
                   preferenceResult=preferencehitDetails.getSourceAsMap.asInstanceOf[util.HashMap[String,Any]]
                   if(preferenceResult.containsKey(courseConfig.notificationPreference)){
                     val notificationPreference: util.HashMap[String, Any] = result.get(courseConfig.notificationPreference).asInstanceOf[util.HashMap[String, Any]]
                     if(MapUtils.isNotEmpty(notificationPreference)){
                       latestCourseAlert=notificationPreference.get(courseConfig.latestCourseAlert).asInstanceOf[Boolean]
                     }
                   }
                 })
                 if (StringUtils.isNotBlank(email) && !excludeEmailsList.contains(email) && latestCourseAlert==false) {
                   if (courseConfig.MAIL_LIST != null && !courseConfig.MAIL_LIST.contains(email)) {
                     emails.add(email)
                   } else {
                     logger.info("Invalid Email :" + email)
                   }
                 }
               }
             }
           }
         })
         logger.info("Email list" + emails)
         /*if(CollectionUtils.isNotEmpty(emails)){
           val kafkaProducerProps = new Properties()
           kafkaProducerProps.put("bootstrap.servers", courseConfig.BOOTSTRAP_SERVER_CONFIG)
           kafkaProducerProps.put("key.serializer", classOf[StringSerializer].getName)
           kafkaProducerProps.put("value.serializer", classOf[StringSerializer].getName)
           val producer = new KafkaProducer[String, String](kafkaProducerProps)
           val producerData = new util.HashMap[String, Any]
           producerData.put(courseConfig.EMAILS, emails)
           producerData.put(courseConfig.PARAMS, params)
           producerData.put(courseConfig.emailTemplate, courseConfig.NEW_COURSES)
           producerData.put(courseConfig.emailSubject, courseConfig.NEW_COURSES_MAIL_SUBJECT)
           producer.send(new ProducerRecord[String, String](courseConfig.kafkaOutPutStreamTopic, courseConfig.DATA, producerData.toString))
         }*/
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
   }*/

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
      val deleteQuery = QueryBuilder.delete().from(courseConfig.sunbird_keyspace, courseConfig.EMAIL_RECORD_TABLE).where(QueryBuilder.eq(courseConfig.EMAIL_TYPE, courseConfig.NEW_COURSES_EMAIL)).toString
      cassandraUtil.delete(deleteQuery)
      val insertQuery = QueryBuilder.insertInto(courseConfig.sunbird_keyspace, courseConfig.EMAIL_RECORD_TABLE)
        .value(courseConfig.EMAIL_TYPE,courseConfig.NEW_COURSES_EMAIL).value(courseConfig.LAST_SENT_DATE, LocalDate.now().toString).toString
      cassandraUtil.upsert(insertQuery)
    } catch {
      case e: Exception => e.printStackTrace()
        logger.info("Update Email Record in Data base failed " + e.getMessage)
    }
  }
}