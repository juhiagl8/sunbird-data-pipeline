package org.sunbird.notificationEngine.task

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.dp.core.job.FlinkKafkaConnector
import org.sunbird.dp.core.util.FlinkUtil
import org.sunbird.notificationEngine.domain.Event
import org.sunbird.notificationEngine.functions.NotificationEngineFunction

import java.io.File

class NotificationEngineTask(config: NotificationEngineEmailConfig, kafkaConnector: FlinkKafkaConnector) {

  private val serialVersionUID = -7729362727131516112L

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

    val source = {
      kafkaConnector.kafkaEventSource[Event](config.inputTopic)
    }

    val stream =
      env.addSource(source, config.NotificationEngineEmailConsumer).uid(config.NotificationEngineEmailConsumer).rebalance()
        .process(new NotificationEngineFunction(config)).setParallelism(config.NotificationEngineParalleism)
        .name(config.NOTIFICATION_ENGINE_FUNCTION).uid(config.NOTIFICATION_ENGINE_FUNCTION)
    stream.getSideOutput(config.updateSuccessEventsOutputTag).addSink(kafkaConnector.kafkaEventSink[Event](config.kafkaSuccessTopic))
      .name(config.successIssueEventSink).uid(config.successIssueEventSink)
      .setParallelism(config.NotificationEngineParalleism)
    stream.getSideOutput(config.failedEvent).addSink(kafkaConnector.kafkaEventSink[Event](config.kafkaIssueTopic))
      .name(config.issueEventSink).uid(config.issueEventSink)
      .setParallelism(config.NotificationEngineParalleism)
    env.execute(config.jobName)
  }
}

object NotificationEngineTask {
  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("courseConfig.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("notificationEngineConfig.conf").withFallback(ConfigFactory.systemEnvironment()))
    val configure = new NotificationEngineEmailConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(configure)
    val task = new NotificationEngineTask(configure, kafkaUtil)
    task.process()
  }
}



