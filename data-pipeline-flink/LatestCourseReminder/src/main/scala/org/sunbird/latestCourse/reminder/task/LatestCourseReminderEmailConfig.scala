package org.sunbird.latestCourse.reminder.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.dp.core.job.BaseJobConfig
import org.sunbird.latestCourse.reminder.domain.Event

class LatestCourseReminderEmailConfig(override val config: Config) extends BaseJobConfig(config, "LatestCourseEmailJob"){
  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val inputTopic: String = config.getString("kafka.input.topic")

  // rating specific
  val latestCourseParallelism: Int = config.getInt("task.latest.course.parallelism")
  val kafkaIssueTopic: String = config.getString("kafka.output.topic")
  val kafkaSuccessTopic: String = config.getString("kafka.success.topic")
  val kafkaOutPutStreamTopic: String = config.getString("kafka.streaming.topic")
  val BOOTSTRAP_SERVER_CONFIG:String=config.getString("kafka.bootstrap_server")

  val issueEventSink = "incomplete-course-email-issue-event-sink"
  val successIssueEventSink = "success-incompletecourse-issue-event-sink"
  val issueOutputTagName = "assessment-sumit-issue-events"
  val failedEvent: OutputTag[Event] = OutputTag[Event]("failed-incompletecourse-email-events")
  val failedEventsOutputTag: OutputTag[Event] = OutputTag[Event]("assess-submit-failed-events")
  val updateSuccessEventsOutputTag: OutputTag[Event] = OutputTag[Event]("update-success-event-count")
  val dbUpdateCount = "db-update-count"
  val dbReadCount = "db-read-count"
  val batchSuccessCount = "batch-success-event-count"
  val failedEventCount = "failed-event-count"
  val ignoredEventsCount = "ignored-event-count"
  val skippedEventCount = "skipped-event-count"
  val cacheHitCount = "cache-hit-count"
  val cacheHitMissCount = "cache-hit-miss-count"
  val certIssueEventsCount = "cert-issue-events-count"
  val dbScoreAggUpdateCount = "db-score-update-count"
  val dbScoreAggReadCount = "db-score-read-count"
  val apiHitSuccessCount = "api-hit-success-count"
  val apiHitFailedCount = "api-hit-failed-count"
  val recomputeAggEventCount = "recompute-agg-event-count"
  val updateCount = "update-count"


  val relationCacheNode: Int = config.getInt("redis.database.relationCache.id")
  val contentCacheNode: Int = config.getInt("redis.database.contentCache.id")

  //Cassandra
  val dbHost: String = config.getString("ext-cassandra.host")
  val dbPort: Int = config.getInt("ext-cassandra.port")
  val sunbird_keyspace: String =config.getString("ext-cassandra.sunbird_keyspace")
  val EMAIL_RECORD_TABLE: String =config.getString("ext-cassandra.EMAIL_RECORD_TABLE")

  val SEND_LATEST_COURSES_ALERT = "send.latest.courses.alert"
  val SEARCH_FIELDS: String = config.getString{"fields.search_fields"}
  val MIN = "min"
  val EMAIL_TYPE = "emailtype"
  val NEW_COURSES_EMAIL = "newcourses"
  val LAST_SENT_DATE = "lastsentdate"
  val MAX = "max"
  val PRIMARY_CATEGORY = "primaryCategory"
  val COURSE = "Course"
  val CONTENT_TYPE_SEARCH = "contentType"
  val LAST_UPDATED_ON = "lastUpdatedOn"
  val DESCENDING_ORDER = "desc"
  val FILTERS = "filters"
  val OFFSET = "offset"
  val LIMIT = "limit"
  val SORT_BY = "sort_by"
  val FIELDS = "fields"
  val REQUEST = "request"
  val NO_OF_COURSES = "noOfCourses"
  val COURSE_KEYWORD = "course"
  val _URL = "_url"
  val THUMBNAIL = "_thumbnail"
  val _NAME = "_name"
  val _DURATION = "_duration"
  val _DESCRIPTION = "_description"
  val new_courses_scheduler_time_gap=168
  val new_courses_email_limit=8

  //url
  val KM_BASE_HOST: String =config.getString("url.km_base_host")
  val content_search: String =config.getString("url.content_search")
  val COURSE_URL: String =config.getString( "url.course_url")
  val SB_SERVICE_URL :String =config.getString("url.sb_service_url")
  val SUNBIRD_USER_SEARCH_ENDPOINT:String=config.getString("url.sunbird_user_search_endpoint")
  val MAIL_LIST:String=config.getString("mailList.recipient_new_course_email")
  val SENDER_MAIL:String=config.getString("senderMail.sender_mail")
  val notification_service_host:String=config.getString("url.notification_service_host")
  val notification_event_endpoint:String=config.getString("url.notification_event_endpoint")
  val SB_API_KEY :String=config.getString("key.sb_api_key")
  val NEW_COURSES = "newcourses"
  val NEW_COURSES_MAIL_SUBJECT = "Check out exciting new courses that launched this week!"
  val NOTIFICATIONS = "notifications"
  val EMAIL = "email"
  val EMAILS = "emails"
  val PARAMS = "params"
  val MESSAGE="message";
  val CONTENT_TYPE = "Content-Type"
  val AUTHORIZATION = "authorization"
  val APPLICATION_JSON = "application/json"

  val GET_USER_EMAIL_LIST_FROM_ES = "get.user.email.list.from.es"
  val EXCLUDE_USER_EMAILS = "exclude_user_emails"
  val STATUS = "status"
  val IS_DELETED = "isDeleted"
  val EMAIL_SEARCH_FIELDS: String = config.getString("fields.email_search_fields")
  val OK = "OK"
  val RESPONSE_CODE = "responseCode"
  val RESULT = "result"
  val RESPONSE = "response"
  val CONTENT = "content"
  val COUNT = "count"
  val PROFILE_DETAILS = "profileDetails"
  val PERSONAL_DETAILS = "personalDetails";
  val PRIMARY_EMAIL = "primaryEmail"
  val latest_courses_alert_content_min_limit=1
  val latest_courses_alert_send_to_all_user:Boolean=config.getBoolean("const.latest_courses_alert_send_to_all_user")
  val DATA="data"
  val emailTemplate="emailTemplate"
  val TEMPLATE="template"
  val emailSubject="emailSubject"
  val SUBJECT="subject"
  val fields="profileDetails.personalDetails.primaryEmail"
  val userId="_id"
  val IDS="ids"
  val PREFERENCELIST="notificationPreference.latestCourseAlert"
  val notificationPreference="notificationPreference"
  val latestCourseAlert="latestCourseAlert"
  val BROAD_CAST_TOPIC_NOTIFICATION_MESSAGE = "BroadCast Topic Notification"
  val ACTOR_TYPE_VALUE = "System"
  val EID_VALUE = "BE_JOB_REQUEST"
  val ACTION="action"
  val BROAD_CAST_TOPIC_NOTIFICATION_KEY = "broadcast-topic-notification-all"
  val iteration="iteration"
  val rawData="rawData"
  val SENDER="sender"
  val TOPIC="topic"
  val OTP="otp"
  val CONFIG="config"
  val DELIVERY_TYPE="deliveryType"
  val DELIVERY_MODE="mode"
  val X_REQUEST_ID = "X-Request-ID"
  val X_TRACE_ENABLED  = "X-Trace-Enabled"
  val VER="ver"
  val ID="id"
  val PDATA="pdata"
  val PRODUCER_ID = "NS"
  val TYPE="type"
  val OBJECT="object"
  val TYPE_VALUE = "TopicNotifyAll"
  val ACTOR="actor"
  val EDATA="edata"
  val EID="eid"
  val TRACE="trace"
  val CONTEXT="context"
  val MID="mid"
  val NOTIFICATION="notification"
  val FROM_EMAIL = "fromEmail"
  val orgImageUrl="orgImageUrl"
  //ES
  val sb_es_user_profile_index:String=config.getString("ES.sb_es_user_profile_index")
  val es_profile_index_type:String=config.getString("ES.es_profile_index_type")
  val sb_es_user_notification_preference: String = config.getString("ES.sb_es_user_notification_preference")
  val es_preference_index_type: String = config.getString("ES.es_preference_index_type")

  val sunbird_mail_server_from_email= "support@igot-dev.in"
  // Consumers
  val LatestCourseReminderEmailConsumer = "latest-course-reminder-email-consumer"

  // Functions
  val latestCourseReminderEmailFunction = "latestCourseReminderEmailFunction"
}
