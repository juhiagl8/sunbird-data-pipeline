package org.sunbird.latestCourse.reminder.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.dp.core.job.FlinkKafkaConnector
import org.sunbird.dp.core.util.FlinkUtil
import org.sunbird.latestCourse.reminder.domain.Event
import org.sunbird.latestCourse.reminder.functions.latestCourseReminderEmailFunction

import java.io.File

class LatestCourseReminderEmailTask(config: LatestCourseReminderEmailConfig, kafkaConnector: FlinkKafkaConnector) {

  private val serialVersionUID = -7729362727131516112L

  def process(): Unit = {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

    val source = {
      kafkaConnector.kafkaEventSource[Event](config.inputTopic)
    }

    val stream =
      env.addSource(source, config.LatestCourseReminderEmailConsumer).uid(config.LatestCourseReminderEmailConsumer).rebalance()
        .process(new latestCourseReminderEmailFunction(config)).setParallelism(config.latestCourseParallelism)
        .name(config.latestCourseReminderEmailFunction).uid(config.latestCourseReminderEmailFunction)
    stream.getSideOutput(config.updateSuccessEventsOutputTag).addSink(kafkaConnector.kafkaEventSink[Event](config.kafkaSuccessTopic))
      .name(config.successIssueEventSink).uid(config.successIssueEventSink)
      .setParallelism(config.latestCourseParallelism)
    stream.getSideOutput(config.failedEvent).addSink(kafkaConnector.kafkaEventSink[Event](config.kafkaIssueTopic))
      .name(config.issueEventSink).uid(config.issueEventSink)
      .setParallelism(config.latestCourseParallelism)
    env.execute(config.jobName)
  }

}

object LatestCourseReminderEmailTask {
  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("courseConfig.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("latestCourseReminder.conf").withFallback(ConfigFactory.systemEnvironment()))
    val latestCourseReminderConfig = new LatestCourseReminderEmailConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(latestCourseReminderConfig)
    val task = new LatestCourseReminderEmailTask(latestCourseReminderConfig, kafkaUtil)
    task.process()
  }
}
