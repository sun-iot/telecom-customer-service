package com.ci123.util

import com.ci123.constant.ConfigConstant
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved 
 *
 * Project: telecom-customer-service
 * Package: com.ci123.util
 * Version: 1.0
 *
 * Created by SunYang on 2019/9/7 19:23
 */
object KafkaUtil {
  var kafkaMap = Map(
    "bootstrap.servers"->ConfigConstant.getVal("cs.kafka.brokers.list") ,
    "key.deserializer"->classOf[StringDeserializer] ,
    "value.deserializer"->classOf[StringDeserializer] ,
    "group.id"->"call_data_collect" ,
    "auto.offset.reset" -> "latest",
    // 自动维护Kafka的偏移量
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  def getKafkaDStream(topic:String , ssc :StreamingContext): InputDStream[ConsumerRecord[String , String]] ={
    KafkaUtils.createDirectStream[String , String](
      ssc ,
      LocationStrategies.PreferConsistent ,
      ConsumerStrategies.Subscribe[String,String](Array(topic) , kafkaMap)
    )
  }


}
