package com.ci123.consumer

import java.util.Properties

import com.ci123.hbase.in.HBaseProcess
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.common.serialization.SimpleStringSchema

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved 
 *
 * Project: telecom-customer-service
 * Package: com.ci123.consumer
 * Version: 1.0
 *
 * Created by SunYang on 2019/10/31 10:06
 */
object KafkaConsumer {

  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(3)
    properties.setProperty("bootstrap.servers" , "hadoop101:9092,hadoop102:9092,hadoop103:9092")
    properties.setProperty("group.id" , "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val callData: DataStream[String] = environment.addSource(new FlinkKafkaConsumer011[String]( "call_data_collect" , new SimpleStringSchema() , properties))

    callData.process(new HBaseProcess())


    callData.print()

    environment.execute()

  }

}
