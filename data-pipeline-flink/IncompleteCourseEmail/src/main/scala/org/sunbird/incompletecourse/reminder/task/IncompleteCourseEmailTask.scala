package org.sunbird.incompletecourse.reminder.task

import java.io.File
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.dp.core.job.FlinkKafkaConnector
import org.sunbird.dp.core.util.FlinkUtil
import org.sunbird.incompletecourse.reminder.domain.Event
import org.sunbird.incompletecourse.reminder.functions.incompleteCourseEmailFunction

class IncompleteCourseEmailTask(config: IncompleteCourseEmailConfig, kafkaConnector: FlinkKafkaConnector) {

  private val serialVersionUID = -7729362727131516112L

  def process(): Unit = {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

    val source = {
      kafkaConnector.kafkaEventSource[Event](config.inputTopic)
    }

    val stream =
      env.addSource(source, config.IncompleteCourseEmailConsumer).uid(config.IncompleteCourseEmailConsumer).rebalance()
        .process(new incompleteCourseEmailFunction(config)).setParallelism(config.ratingParallelism)
        .name(config.incompleteCourseEmailFunction).uid(config.incompleteCourseEmailFunction)
    stream.getSideOutput(config.updateSuccessEventsOutputTag).addSink(kafkaConnector.kafkaEventSink[Event](config.kafkaSuccessTopic))
      .name(config.successIssueEventSink).uid(config.successIssueEventSink)
      .setParallelism(config.ratingParallelism)
    stream.getSideOutput(config.failedEvent).addSink(kafkaConnector.kafkaEventSink[Event](config.kafkaIssueTopic))
      .name(config.issueEventSink).uid(config.issueEventSink)
      .setParallelism(config.ratingParallelism)
    env.execute(config.jobName)
  }

}

object IncompleteCourseEmailTask {
  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("courseReminder.conf").withFallback(ConfigFactory.systemEnvironment()))
    val assessmentConfig = new IncompleteCourseEmailConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(assessmentConfig)
    val task = new IncompleteCourseEmailTask(assessmentConfig, kafkaUtil)
    task.process()
  }
}
