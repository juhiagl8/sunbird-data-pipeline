package org.sunbird.incompletecourse.reminder.domain

import org.sunbird.dp.core.domain.{Events, EventsPath}

import java.util

class Event(eventMap: util.Map[String, Any]) extends Events(eventMap) {
  private val jobName = "IncompleteCourseEmailJob"

  def message: String = {
    telemetry.read[String]("message").get
  }

  def markFailed(errorMsg: String): Unit = {
    telemetry.addFieldIfAbsent(EventsPath.FLAGS_PATH, new util.HashMap[String, Boolean])
    telemetry.addFieldIfAbsent("metadata", new util.HashMap[String, AnyRef])
    telemetry.add("metadata.validation_error", errorMsg)
    telemetry.add("metadata.src", jobName)
  }

}
