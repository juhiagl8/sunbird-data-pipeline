package org.sunbird.notification.preference.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.dp.core.job.BaseJobConfig
import org.sunbird.notification.preference.domain.Event

class NotificationPreferenceConfig (override val config: Config) extends BaseJobConfig(config, "notificationPreference") {
  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val inputTopic: String = config.getString("kafka.input.topic")

  //kafkaOutput topic
  val NOTIFICATION_JOB_Topic: String = config.getString("kafka.notification_job_topic")

  val notificationPreferenceParallelism: Int = config.getInt("task.notificationPreference.parallelism")
  val kafkaIssueTopic: String = config.getString("kafka.output.topic")
  val issueEventSink = "notification-preference-issue-event-sink"
  val issueOutputTagName = "notification-preference-issue-events"
  val failedEvent: OutputTag[Event] = OutputTag[Event]("failed-notification-preference-events")
  val key_serializer: String = config.getString("kafka-key.key_serializer")
  val value_serializer: String = config.getString("kafka-key.value_serializer")
  val bootstrap_servers:String=config.getString("kafka-key.bootstrap_servers")
  val BOOTSTRAP_SERVER_CONFIG:String=config.getString("kafka.bootstrap_server")

  //ES
  val sb_es_user_notification_preference: String = config.getString("ES.sb_es_user_notification_preference")
  val es_preference_index_type: String = config.getString("ES.es_preference_index_type")
  val ES_HOST:String=config.getString("ES.host")
  val ES_PORT:String=config.getString("ES.port")

  //Cassandra
  val dbHost: String = config.getString("ext-cassandra.host")
  val dbPort: Int = config.getInt("ext-cassandra.port")

  // Consumers
  val NotificationPreferenceConsumer = "notification-preference-consumer"

  // Functions
  val notificationPreferenceFunction = "NotificationPreferenceFunction"

  //constant
  val CHECK_NOTIFICATION_PREFERENCE_KEY="checkNotificationPreferenceKey"
  val SENDER_MAIL:String=config.getString("senderMail.sender_mail")


  //fields
  val USERID="userId"
  val BROAD_CAST_TOPIC_NOTIFICATION_MESSAGE = "BroadCast Topic Notification"
  val ACTOR_TYPE_VALUE = "System"
  val EID_VALUE = "BE_JOB_REQUEST"
  val ACTION = "action"
  val BROAD_CAST_TOPIC_NOTIFICATION_KEY = "broadcast-topic-notification-all"
  val iteration = "iteration"
  val rawData = "rawData"
  val SENDER = "sender"
  val TOPIC = "topic"
  val OTP = "otp"
  val CONFIG = "config"
  val DELIVERY_TYPE = "deliveryType"
  val DELIVERY_MODE = "mode"
  val X_REQUEST_ID = "X-Request-ID"
  val X_TRACE_ENABLED = "X-Trace-Enabled"
  val VER = "ver"
  val ID = "id"
  val PDATA = "pdata"
  val PRODUCER_ID = "NS"
  val TYPE = "type"
  val OBJECT = "object"
  val TYPE_VALUE = "TopicNotifyAll"
  val ACTOR = "actor"
  val EDATA = "edata"
  val EID = "eid"
  val TRACE = "trace"
  val CONTEXT = "context"
  val MID = "mid"
  val PARAMS="params"
  val DATA="data"
  val TEMPLATE="template"
  val IDS="ids"
  val REQUEST="request"
  val NOTIFICATION="notification"
  val SUBJECT="subject"
  val EMAIL = "email"
  val MESSAGE="message"
  val latestCourseAlert="latestCourseAlert"
}
