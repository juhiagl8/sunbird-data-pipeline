package org.sunbird.notification.preference.task

import java.io.File
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.dp.core.job.FlinkKafkaConnector
import org.sunbird.dp.core.util.FlinkUtil
import org.sunbird.notification.preference.domain.Event
import org.sunbird.notification.preference.function.NotificationPreferenceFunction

class NotificationPreferenceTask(config: NotificationPreferenceConfig, kafkaConnector: FlinkKafkaConnector) {

  private val serialVersionUID = -7729362727131516112L

  def process(): Unit = {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

    val source = kafkaConnector.kafkaEventSource[Event](config.inputTopic)

    val stream =
      env.addSource(source, config.NotificationPreferenceConsumer).uid(config.NotificationPreferenceConsumer).rebalance()
      .process(new NotificationPreferenceFunction(config)).setParallelism(config.notificationPreferenceParallelism)
      .name(config.notificationPreferenceFunction).uid(config.notificationPreferenceFunction)
    stream.getSideOutput(config.failedEvent).addSink(kafkaConnector.kafkaEventSink[Event](config.kafkaIssueTopic))
      .name(config.issueEventSink).uid(config.issueEventSink)
      .setParallelism(config.notificationPreferenceParallelism)
    env.execute(config.jobName)
  }
}

object NotificationPreferenceTask {
  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("notificationPreference-config.conf").withFallback(ConfigFactory.systemEnvironment()))
    val ratingConfig = new NotificationPreferenceConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(ratingConfig)
    val task = new NotificationPreferenceTask(ratingConfig, kafkaUtil)
    task.process()
  }
}

