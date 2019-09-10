package com.ci123.service;

import com.ci123.bean.Emp;
import com.ci123.util.SparkESUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: telecom-customer-service
 * Package: com.ci123.service
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/10 9:02
 */
public class SparkES {

    public static void main(String[] args) {

        SparkConf conf = SparkESUtil.createSparkConf(Thread.currentThread().getStackTrace()[0].getClassName());

        JavaSparkContext context = SparkESUtil.createJavaSparkContext(conf);

        SparkSession session = SparkESUtil.createSparkSession(conf);

        Dataset emp = SparkESUtil.createDataset(session, "emp");
        emp.registerTempTable("emp");
        String empSql = "select * from emp" ;
        Dataset<Row> empDataset = session.sql(empSql);
        empDataset.show(3);


        JavaRDD<Emp> empMap = empDataset.javaRDD().map(new Function<Row, Emp>() {
            @Override
            public Emp call(Row v1) throws Exception {
                return new Emp(
                        v1.getString(0) ,
                        v1.getString(1) ,
                        Integer.parseInt(v1.getString(2)) ,
                        v1.getString(3),
                        v1.getString(4),
                        v1.getString(5) ,
                        v1.getString(6));
            }
        });

        // JavaEsSpark.saveToEs(empMap , "hbase-spark/emp");
        SparkESUtil.insertES(empMap , "hbase-spark/emp");

        session.stop();
        context.stop();

    }
}
