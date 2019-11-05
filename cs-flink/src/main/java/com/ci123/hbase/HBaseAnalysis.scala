package com.ci123.hbase

import com.alibaba.fastjson.JSON
import com.ci123.hbase.out.HBaseRead
import com.ci123.util.JSONMap
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved 
 *
 * Project: telecom-customer-service
 * Package: com.ci123.hbase
 * Version: 1.0
 *
 * Created by SunYang on 2019/11/4 16:00
 */
object HBaseAnalysis {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val resultFromHBase: DataStream[String] = environment.addSource(new HBaseRead(("telecom-customer-service")))

    val callValue: DataStream[CallInformation] = resultFromHBase.map(
      recends => {
        val values: Array[String] = recends.split(",")
        CallInformation(values(0), values(1), values(2), values(3), values(4), values(5).toLong, values(6).toLong, values(7).toLong)
      }
    )
    val value: DataStream[CallResult] = callValue.map(
      recends => {
        CallResult(recends.phoneNumberA + "_" + recends.phoneNumberB, recends.callA, recends.callB, recends.duration.toInt, 1)
      }
    )
    val valueSum: DataStream[CallResult] = value
      .keyBy(0)
      .reduce((t1, t2) => {
        CallResult(t1.rowKey, t1.callA, t1.callB, t1.duration + t2.duration, t1.count + t2.count)
      })

    var hosts = new java.util.ArrayList[HttpHost]
    hosts.add(new HttpHost("192.168.1.111", 9200, "http"))

    val elastatic = new ElasticsearchSink.Builder[CallResult](
      hosts,
      new ElasticsearchSinkFunction[CallResult] {
        override def process(callResult: CallResult, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          requestIndexer.add(new IndexRequest().index("telecom-customer-service-count").`type`("default").source(JSONMap.jsonToMap(callResult)))
        }
      }
    )
    elastatic.setBulkFlushBackoff(true)
    valueSum.addSink(elastatic.build())
    environment.execute()
  }
}

case class CallInformation(
                            var rowKey: String,
                            var callA: String,
                            var callB: String,
                            var dateTime: String,
                            var duration: String,
                            var phoneNumberA: Long,
                            var phoneNumberB: Long,
                            var timestamp: Long
                          ) {

}
case class CallResult(
                            var rowKey: String,
                            var callA: String,
                            var callB: String,
                            var duration: Int,
                            var count: Int
                          ) {
}