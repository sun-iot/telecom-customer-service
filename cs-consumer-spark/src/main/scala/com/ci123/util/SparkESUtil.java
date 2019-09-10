package com.ci123.util;

import com.ci123.constant.ConfigConstant;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: telecom-customer-service
 * Package: com.ci123.util
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/10 8:49
 */
public class SparkESUtil {
    private static SparkConf conf = null ;
    private static SparkSession session = null ;
    /**
     * 返回一个配置文件，方便后面添加新的应用
     * @return
     */
    public static SparkConf createSparkConf(String name){
        if (conf == null ){
            conf = new SparkConf().setAppName(name)
                    .setMaster("local[*]") ;
        }
        return conf ;

    }

    /**
     * 返回一个上下文
     * @return
     */
    public static JavaSparkContext createJavaSparkContext(SparkConf sparkConf){

        return new JavaSparkContext(
                sparkConf.set("es.index.auto.create","true")
                .set("es.nodes","192.168.1.111")
                .set("es.port","9200")
        );
    }

    /**
     * 返回一个SaprkSession 对象
     * @param sparkConf
     * @return
     */
    public static SparkSession createSparkSession(SparkConf sparkConf){

        synchronized (Object.class){
            if (session == null){
                session =  SparkSession.builder()
                        .config(sparkConf)
                        .config( "spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                        .config("spark.broadcast.compress", "false")
                        .config("spark.shuffle.compress", "false")
                        .config("spark.shuffle.spill.compress", "false")
                        .config("spark.driver.host", "localhost")
                        .getOrCreate();
            }
        }
        return session ;
    }

    /**
     * 返回一个通过Phoinex获取到的HBase数据集
     * @param session
     * @param table
     * @return
     */
    public static Dataset createDataset(SparkSession session ,String table){
        return session.read()
                .format(ConfigConstant.getVal("cs.phoinex.driver"))
                .option("table" ,table)
                .option("zkUrl" , ConfigConstant.getVal("cs.phoinex.zkurl"))
                .load();
    }

    /**
     * 插入到ES中
     * @param rdd
     * @param resource
     */
    public static void insertES(JavaRDD rdd , String resource) {
        JavaEsSpark.saveToEs(rdd , resource);
    }
}
