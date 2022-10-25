package org.sunbird.incompletecourse.reminder.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.dp.core.job.BaseJobConfig
import org.sunbird.incompletecourse.reminder.domain.Event

class IncompleteCourseEmailConfig(override val config: Config) extends BaseJobConfig(config, "IncompleteCourseEmailJob"){
  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val inputTopic: String = config.getString("kafka.input.topic")

  // rating specific
  val ratingParallelism: Int = config.getInt("task.rating.parallelism")
  val kafkaIssueTopic: String = config.getString("kafka.output.topic")
  val kafkaSuccessTopic: String = config.getString("kafka.success.topic")
  val BOOTSTRAP_SERVER_CONFIG: String = config.getString("kafka.bootstrap_server")
  val kafkaOutPutStreamTopic: String = config.getString("kafka.streaming.topic")

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
  val USER_CONTENT_DB_TABLE : String = config.getString("ext-cassandra.user_content_db_table")
  val dbHost: String = config.getString("ext-cassandra.host")
  val dbPort: Int = config.getInt("ext-cassandra.port")
  val dbCoursesKeyspace: String = config.getString("ext-cassandra.courses_keyspace")
  val dbSunbirdKeyspace: String = config.getString("ext-cassandra.keyspace")
  val dev_hierarchy_store_keyspace: String =config.getString("ext-cassandra.dev_hierarchy_store_keyspace")
  val content_hierarchy_table:String=config.getString("ext-cassandra.content_hierarchy_table")
  val TABLE_USER:String =config.getString("ext-cassandra.user_table")
  val EXCLUDE_USER_EMAILS:String =config.getString("ext-cassandra.exclude_user_emails_table")
  val NOTIFICATION_HOST :String=config.getString( "url.notification_service_host")
  val sender_mail : String=config.getString("mail.sender_mail")
  val notification_event_endpoint : String=config.getString("mail.notification_event_endpoint")


  val last_access_time_gap_millis=259200000

  val courseid ="courseid"
  val IDENTIFIER = "identifier"
  val HIERARCHY = "hierarchy"
  val COURSE_ID = "courseId"
  val NAME = "name"
  val POSTER_IMAGE = "posterImage"
  val Thumbnail="Thumbnail"
  val ID = "id"
  val PROFILE_DETAILS_KEY = "profiledetails"
  val PERSONAL_DETAILS_KEY = "personalDetails"
  val COURSE_URL: String =config.getString("url.course_url")
  val OVERVIEW_BATCH_ID: String = config.getString("url.overview_batch")
  val EMAIL = "email"
  val PRIMARY_EMAIL = "primaryEmail"
  val COURSE_KEYWORD = "course"
  val _URL = "_url"
  val THUMBNAIL = "_thumbnail"
  val _NAME = "_name"
  val _DURATION = "_duration";
  val INCOMPLETE_COURSES_MAIL_SUBJECT = "Complete the courses you started";
  val INCOMPLETE_COURSES = "incompletecourses";
  val MESSAGE="message";
  val NOTIFICATIONS = "notifications"
  val REQUEST = "request"
  val EMAILS = "emails"
  val PARAMS = "params"
  val DATA = "data"
  val emailTemplate = "emailTemplate"
  val emailSubject = "emailSubject"
  // Consumers
  val IncompleteCourseEmailConsumer = "incomplete-course-email-consumer"

  // Functions
  val incompleteCourseEmailFunction = "IncompleteCourseEmailFunction"
}
