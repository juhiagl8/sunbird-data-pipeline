package org.sunbird.incompletecourse.reminder.util

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import HttpMethods._
import akka.stream.ActorMaterializer
import org.apache.http.client.methods.{HttpPost, HttpRequestBase}
import org.apache.http.impl.client.{DefaultHttpClient, HttpClients}
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.apache.http.entity.{BasicHttpEntity, ByteArrayEntity, SerializableEntity, StringEntity}
import org.slf4j.LoggerFactory


class RestApiUtil extends Serializable{


  private[this] val logger = LoggerFactory.getLogger(classOf[RestApiUtil])
  logger.info("RestApi Call start")

  def postRequest(uri: String, params: java.util.Map[String, Any]):Unit={
    logger.info("rest api method start"+ uri)
    val post=new HttpPost(uri)
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.setVisibility(PropertyAccessor.ALL, Visibility.ANY)
    val jsonString=mapper.writeValueAsString(params)
    post.setHeader("Content-type", "application/json")
    post.setEntity(new StringEntity(jsonString))
    val client=new DefaultHttpClient()
    val response=client.execute(post)
    val statusCode=response.getStatusLine.getStatusCode
    if (statusCode.equals(200)) {
      logger.info("Rest Call successfully working")
    }
  }
}
