package org.sunbird.latestCourse.reminder.util

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import java.util.Properties

class ReadValue {

  private[this] val logger = LoggerFactory.getLogger(classOf[ReadValue])

  def readValue(key:String):String={
    logger.info("Entering ReadValue")
    if(StringUtils.isBlank(key)){
      logger.info("Provided key is either null or emapty :" + key);
      return null
    }
    var value: String = System.getenv(key)
    if(StringUtils.isBlank(value)){
      value=getProperty(key)
    }
    logger.info("found value for key:" + key + " value: " + value);
    value
  }

  def getProperty(key:String):String={
    logger.info("Entering getProperty")
    val value:String=System.getenv(key)
    if (StringUtils.isNotBlank(key)){
     return value
    }
    val prop : Properties=new Properties()
    if(prop.getProperty(key)!=null){
      logger.info("Key from getProperty "+prop.getProperty(key))
      return prop.getProperty(key)
    }else{
      key
    }
  }
}
