package com.ci123.service

import com.ci123.bean.{CustomerCall, CustomerError}
import com.ci123.util.KafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.Option

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved 
 *
 * Project: telecom-customer-service
 * Package: com.ci123.service
 * Version: 1.0
 *
 * Created by SunYang on 2019/9/7 19:08
 */
object SparkKafka {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("telecom-customer-service").setMaster("local[*]").set("spark.driver.host", "127.0.0.1")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setCheckpointDir("telecom-customer-service_ch")
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaDStream("call_data_collect", ssc)

    val customerMap: DStream[Any] = kafkaDStream.map {
      record => {
        val msg: String = record.value()
        val datas: Array[String] = msg.split(",")
        if (datas.length < 7) {
          print("datas.leagel", "the length is error")
        } else {
          // Integer.parseInt(datas(6))
          CustomerCall(datas(0), datas(1), datas(2), datas(3), datas(4), datas(5), Integer.valueOf(datas(6)).toLong )
        }
      }
    }

    // 统计主叫与被叫之间通话次数最多的10组，以及每组累计通话的时间长度
    val countMap: DStream[(String, Long)] = customerMap.mapPartitions {
      partition => {
        partition.map {
          comsumer => {
            val customerCall: CustomerCall = comsumer.asInstanceOf[CustomerCall]
            (customerCall.call1_name + "_" + customerCall.call2_name, customerCall.duration)
          }
        }
      }
    }

//    val customerStateDs: DStream[(String, Long)] = countMap.updateStateByKey {
//      (seq: Seq[Long], option: Option[Long]) => {
//        val sum: Long = option.getOrElse(0L) + option.sum
//        Option(sum)
//      }
//    }
    countMap.reduceByKey(_+_)
    .foreachRDD {
      rdd => {
        rdd.foreachPartition {
          partition => {
            partition.foreach {
              case (key, sum) => {
                val str: Array[String] = key.split("_")
                println(str(0) + "->" + str(1) + ":" + sum.toString)
              }
            }
          }
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
