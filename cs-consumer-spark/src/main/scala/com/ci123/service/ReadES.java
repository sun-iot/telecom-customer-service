package com.ci123.service;

import com.ci123.util.SparkESUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: telecom-customer-service
 * Package: com.ci123.service
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/10 10:29
 */
public class ReadES {
    public static void main(String[] args) {
        SparkConf conf = SparkESUtil.createSparkConf(Thread.currentThread().getStackTrace()[0].getClassName());

        JavaSparkContext context = SparkESUtil.createJavaSparkContext(conf);

        JavaPairRDD<String, String> jsonRDD = JavaEsSpark.esJsonRDD(context, "hbase-spark/emp");
        jsonRDD.distinct(1).foreach((VoidFunction<Tuple2<String, String>>) tuple2 -> System.out.println(tuple2._1() + ":" + tuple2._2()));

        context.stop();
    }
}
