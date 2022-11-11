package org.sunbird.notification.preference.function

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.Gson
import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilders}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.slf4j.LoggerFactory
import org.sunbird.dp.contentupdater.core.util.RestUtil
import org.sunbird.dp.core.cache.DataCache
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.dp.core.util.{CassandraUtil, JSONUtil}
import org.sunbird.notification.preference.domain.Event
import org.sunbird.notification.preference.task.NotifiationPreferenceConfig
import org.sunbird.notification.preference.util.IndexService

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util
import java.util.{Properties, UUID}

class NotificationPreferenceFunction(preferenceConfig: NotifiationPreferenceConfig, @transient var cassandraUtil: CassandraUtil = null)(implicit val mapTypeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](preferenceConfig) {

  private[this] val logger = LoggerFactory.getLogger(classOf[NotificationPreferenceFunction])

  private var dataCache: DataCache = _
  private var restUtil: RestUtil = _

  override def metricsList(): List[String] = {
    List()
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(preferenceConfig.dbHost, preferenceConfig.dbPort)
  }

  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {
    try {
      val message=event.message
      if(message.equalsIgnoreCase(preferenceConfig.CHECK_NOTIFICATION_PREFERENCE_KEY)){
        val emailWithUserId:util.List[util.HashMap[String,Any]]=event.emailWithUserId
        val userIdList=new util.ArrayList[String]()
        var userId: util.Set[String]=null
        logger.info("email with UserId "+emailWithUserId)
        emailWithUserId.forEach(data=>{
          userId= data.keySet()
          logger.info("userId only "+userId)
          userId.forEach(id => {
            userIdList.add(id)
            logger.info("userId==> " + userIdList)
          })
        })
        var enableEmailList=new util.ArrayList[String]()
        if(CollectionUtils.isNotEmpty(userIdList)) {
          enableEmailList = initiateNotificationPreferenceCheck(userIdList)
        }
        logger.info("enableEmailList " + enableEmailList)
        val finalEmailList = new util.ArrayList[String]()

        if (CollectionUtils.isNotEmpty(enableEmailList) && enableEmailList != null) {
          val toRemove: util.List[String] = new util.ArrayList[String]()
          logger.info("remove if Block")
          userIdList.forEach(id => {
            if (!enableEmailList.contains(id)) {
              toRemove.add(id)
            }
          })
          logger.info("to remove userId " + toRemove)
          toRemove.forEach(id => {
            emailWithUserId.forEach(mapObj => {
              mapObj.remove(id)
            })
          })
          if (CollectionUtils.isNotEmpty(emailWithUserId)) {
            logger.info("if block for adding final email list")
            emailWithUserId.forEach(mapObj => {
              val entryObj = mapObj.entrySet()
              logger.info("entry data " + entryObj)
              entryObj.forEach(i => {
                val mail = i.getValue
                logger.info("mail " + mail)
                finalEmailList.add(mail.asInstanceOf[String])
              })
            })
          }
        }
        logger.info("emailWithUserId 2 "+emailWithUserId)
        logger.info("final Email List "+finalEmailList)

        if(CollectionUtils.isNotEmpty(finalEmailList)){
          val emailTemplate = event.emailTemplate
          val emailSubject = event.emailSubject
          val params = event.params
          initiateKafkaMessage(finalEmailList,emailTemplate,params,emailSubject)
        }
      }
    }
    catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.info("Event throwing exception: "+ ex.getMessage)
      }
    }
  }

  def initiateNotificationPreferenceCheck(userId: util.ArrayList[String]):util.ArrayList[String] = {
    try {
      val index = new IndexService()
      logger.info("Entering checkNotificationPreferenceByUserId")
      logger.info("request Object " + userId)
      val query: BoolQueryBuilder = QueryBuilders.boolQuery()
      val finalQuery: BoolQueryBuilder = QueryBuilders.boolQuery()
      finalQuery.must(QueryBuilders.termsQuery(preferenceConfig.USERID, userId)).must(QueryBuilders.termQuery(preferenceConfig.latestCourseAlert,true)).must(query)
      logger.info("query "+query)
      val sourceBuilder = new SearchSourceBuilder().query(finalQuery)
      sourceBuilder.fetchSource(preferenceConfig.USERID, new String())
      sourceBuilder.from(0)
      sourceBuilder.size(45)
      logger.info("sourceBuilder " + sourceBuilder)
      val notificationPreferenceResponse = index.getEsResult(preferenceConfig.sb_es_user_notification_preference, preferenceConfig.es_preference_index_type, sourceBuilder, true)
      var preferenceResult = new util.HashMap[String, Any]()
      val enabledUserId = new util.ArrayList[String]
      if(notificationPreferenceResponse.getHits.getTotalHits!=0){
        logger.info("If hit block")
        notificationPreferenceResponse.getHits.forEach(preferencehitDetails => {
          logger.info("hit details double hit " + notificationPreferenceResponse.getHits.getHits.toString)
          logger.info("hit details " + notificationPreferenceResponse.getHits.toString)
          preferenceResult = preferencehitDetails.getSourceAsMap.asInstanceOf[util.HashMap[String, Any]]
          logger.info("preferenceResult "+preferenceResult)
          if (preferenceResult.containsKey(preferenceConfig.USERID)) {
            enabledUserId.add(preferenceResult.get(preferenceConfig.USERID).asInstanceOf[String])
            logger.info("enabled UserId "+enabledUserId)
          }
        })
      }
      return enabledUserId
    }catch {
      case ex: Exception => {
        logger.info("Exception Occurs while getting users Notification Preference : ", ex.getMessage)
      }
        return null
    }
  }


  def initiateKafkaMessage(finalEmailList: util.ArrayList[String], emailTemplate: String, params: util.Map[String, Any], emailSubject: String) = {
    logger.info("Entering InitiateKafkaMessage")
    val Actor = new util.HashMap[String, String]()
    Actor.put(preferenceConfig.ID, preferenceConfig.BROAD_CAST_TOPIC_NOTIFICATION_MESSAGE)
    Actor.put(preferenceConfig.TYPE, preferenceConfig.ACTOR_TYPE_VALUE)
    val eid = preferenceConfig.EID_VALUE
    val edata = new util.HashMap[String, Any]()
    edata.put(preferenceConfig.ACTION, preferenceConfig.BROAD_CAST_TOPIC_NOTIFICATION_KEY)
    edata.put(preferenceConfig.iteration, 1)
    val request = new util.HashMap[String, Any]()
    val config = new util.HashMap[String, String]()
    config.put(preferenceConfig.SENDER, preferenceConfig.SENDER_MAIL)
    config.put(preferenceConfig.TOPIC, null)
    config.put(preferenceConfig.OTP, null)
    config.put(preferenceConfig.SUBJECT, emailSubject)

    val templates = new util.HashMap[String, Any]()
    templates.put(preferenceConfig.DATA, null)
    templates.put(preferenceConfig.ID, emailTemplate)
    templates.put(preferenceConfig.PARAMS, params)
    logger.info("templates " + templates)

    val notification = new util.HashMap[String, Any]()
    notification.put(preferenceConfig.rawData, null)
    notification.put(preferenceConfig.CONFIG, config)
    notification.put(preferenceConfig.DELIVERY_TYPE, preferenceConfig.MESSAGE)
    notification.put(preferenceConfig.DELIVERY_MODE, preferenceConfig.EMAIL)
    notification.put(preferenceConfig.TEMPLATE, templates)
    notification.put(preferenceConfig.IDS, finalEmailList)

    logger.info("notification " + notification)

    request.put(preferenceConfig.NOTIFICATION, notification)
    edata.put(preferenceConfig.REQUEST, request)
    val trace = new util.HashMap[String, Any]()
    trace.put(preferenceConfig.X_REQUEST_ID, null)
    trace.put(preferenceConfig.X_TRACE_ENABLED, false)
    val pdata = new util.HashMap[String, Any]()
    pdata.put(preferenceConfig.VER, "1.0")
    pdata.put(preferenceConfig.ID, "org.sunbird.platform")
    val context = new util.HashMap[String, Any]()
    context.put(preferenceConfig.PDATA, pdata)
    val ets = System.currentTimeMillis()
    val mid = preferenceConfig.PRODUCER_ID + "." + ets + "." + UUID.randomUUID();
    val objectsDetails = new util.HashMap[String, Any]()
    objectsDetails.put(preferenceConfig.ID, getRequestHashed(request, context))
    objectsDetails.put(preferenceConfig.TYPE, preferenceConfig.TYPE_VALUE)

    val producerData = new util.HashMap[String, Any]
    producerData.put(preferenceConfig.ACTOR, Actor)
    producerData.put(preferenceConfig.EDATA, edata)
    producerData.put(preferenceConfig.EID, eid)
    producerData.put(preferenceConfig.TRACE, trace)
    producerData.put(preferenceConfig.CONTEXT, context)
    producerData.put(preferenceConfig.MID, mid)
    producerData.put(preferenceConfig.OBJECT, objectsDetails)

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
        logger.error("Error while encrypting "+context, e);
        value = ""
    }
    value
  }

  def sendMessageToKafkaTopic(producerData: util.HashMap[String, Any]) :Unit= {
    logger.info("Entering SendMessageKafkaTopic")
    if(MapUtils.isNotEmpty(producerData)){
      val kafkaProducerProps = new Properties()
      kafkaProducerProps.put(preferenceConfig.bootstrap_servers, preferenceConfig.BOOTSTRAP_SERVER_CONFIG)
      logger.info("failed here 1")
      kafkaProducerProps.put(preferenceConfig.key_serializer, classOf[StringSerializer])
      kafkaProducerProps.put(preferenceConfig.value_serializer, classOf[StringSerializer])
      val producer = new KafkaProducer[String, String](kafkaProducerProps)
      val mapper: ObjectMapper = new ObjectMapper()
      mapper.setVisibility(PropertyAccessor.ALL, Visibility.ANY)
      val jsonString = mapper.writeValueAsString(producerData)
      logger.info("json String " + jsonString)
      producer.send(new ProducerRecord[String, String](preferenceConfig.NOTIFICATION_JOB_Topic, preferenceConfig.DATA, jsonString))
      logger.info("message Send to kafka")
    }
  }
}

