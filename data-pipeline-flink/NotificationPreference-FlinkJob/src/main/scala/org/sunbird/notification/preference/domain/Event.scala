package org.sunbird.notification.preference.domain

import java.util
import org.sunbird.dp.core.domain.{Events, EventsPath}


class Event(eventMap: util.Map[String, Any]) extends Events(eventMap) {
  private val jobName = "notificationPreference"

  def emailTemplate: String = {
    telemetry.read[String]("emailTemplate").get

  }

  def message: String = {
    telemetry.read[String]("message").get
  }

  def emailSubject: String = {
    telemetry.read[String]("emailSubject").get
  }

  def params:util.Map[String,Any]={
    telemetry.read[util.Map[String,Any]]("params").get
  }

  def emailWithUserId:util.List[util.HashMap[String,Any]]={
    telemetry.read[util.List[util.HashMap[String,Any]]]("emailWithUserId").get
  }
  def markFailed(errorMsg: String): Unit = {
    telemetry.addFieldIfAbsent(EventsPath.FLAGS_PATH, new util.HashMap[String, Boolean])
    telemetry.addFieldIfAbsent("metadata", new util.HashMap[String, AnyRef])
    telemetry.add("metadata.validation_error", errorMsg)
    telemetry.add("metadata.src", jobName)
  }

}
