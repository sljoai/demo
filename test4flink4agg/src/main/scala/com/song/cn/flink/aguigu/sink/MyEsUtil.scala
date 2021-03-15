package com.song.cn.flink.aguigu.sink

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import java.util

object MyEsUtil {

  // 创建ES服务器信息列表
  val httpHostList: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]
  httpHostList.add(new HttpHost("hadoop01",9200))
  httpHostList.add(new HttpHost("hadoop02",9200))
  httpHostList.add(new HttpHost("hadoop03",9200))

  def getEsSink(indexName:String): ElasticsearchSink[StartUpLog] ={
    val indexRequest: ElasticsearchSinkFunction[StartUpLog] = new ElasticsearchSinkFunction[StartUpLog] {
      override def process(element: StartUpLog, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        val strJson = JSON.toJSONString(element, SerializerFeature.PrettyFormat)
        println(strJson)
        val jsonObject = JSON.parseObject(strJson)
        val requestIndexer1: IndexRequest = Requests.indexRequest()
          .index(indexName).`type`("_doc").source(jsonObject)
        requestIndexer.add(requestIndexer1)
      }
    }
    val elasticSearchSinkBuilder = new ElasticsearchSink.Builder[StartUpLog](httpHostList, indexRequest)
    // 刷新前缓冲的最大动作量
    elasticSearchSinkBuilder.setBulkFlushMaxActions(10)
    elasticSearchSinkBuilder.build()
  }

}
