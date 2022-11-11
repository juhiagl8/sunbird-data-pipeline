package org.sunbird.notification.preference.util

import org.apache.commons.lang3.StringUtils
import org.apache.http.HttpHost
import org.elasticsearch.action.search.{MultiSearchRequest, SearchRequest, SearchResponse}
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.slf4j.LoggerFactory

class IndexService {
  private[this] val logger = LoggerFactory.getLogger(classOf[IndexService])

  logger.info("Entering to IndexService ")
  private var esClient : RestHighLevelClient=new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200 )))
  private var sbClient : RestHighLevelClient=new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200)))

  def getEsResult(indexName: String, esType: String, searchSourceBuilder: SearchSourceBuilder, isSunbirdES: Boolean): SearchResponse = {
    val searchRequest = new SearchRequest()
    searchRequest.indices(indexName)
    if (!StringUtils.isEmpty(esType)) {
      searchRequest.types(esType)
    }
    searchRequest.source(searchSourceBuilder)
    logger.info("searchRequest "+searchRequest)
    if (isSunbirdES) {
      logger.info("sbResponse "+sbClient.search(searchRequest, RequestOptions.DEFAULT))
      sbClient.search(searchRequest, RequestOptions.DEFAULT)
    } else {
      logger.info("esResponse "+esClient.search(searchRequest, RequestOptions.DEFAULT))
      esClient.search(searchRequest, RequestOptions.DEFAULT)
    }
  }
}
