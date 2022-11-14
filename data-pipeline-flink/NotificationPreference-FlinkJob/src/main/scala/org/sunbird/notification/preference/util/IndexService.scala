package org.sunbird.notification.preference.util

import org.apache.commons.lang3.StringUtils
import org.apache.http.HttpHost
import org.elasticsearch.action.search.{MultiSearchRequest, SearchRequest, SearchResponse}
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.slf4j.LoggerFactory
import org.sunbird.notification.preference.task.NotificationPreferenceConfig

class IndexService(config:NotificationPreferenceConfig) {
  private[this] val logger = LoggerFactory.getLogger(classOf[IndexService])

  logger.info("Entering to IndexService ")
  private var esClient : RestHighLevelClient=new RestHighLevelClient(RestClient.builder(new HttpHost(config.ES_HOST, config.ES_PORT.toInt)))
  private var sbClient : RestHighLevelClient=new RestHighLevelClient(RestClient.builder(new HttpHost(config.ES_HOST, config.ES_PORT.toInt)))

  def getEsResult(indexName: String, esType: String, searchSourceBuilder: SearchSourceBuilder, isSunbirdES: Boolean): SearchResponse = {
    val searchRequest = new SearchRequest()
    searchRequest.indices(indexName)
    if (!StringUtils.isEmpty(esType)) {
      searchRequest.types(esType)
    }
    searchRequest.source(searchSourceBuilder)
    if (isSunbirdES) {
      sbClient.search(searchRequest, RequestOptions.DEFAULT)
    } else {
      esClient.search(searchRequest, RequestOptions.DEFAULT)
    }
  }
}
