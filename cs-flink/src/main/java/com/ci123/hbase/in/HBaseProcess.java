package com.ci123.hbase.in;

import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: telecom-customer-service
 * Package: com.ci123.hbase.in
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/10/31 17:48
 */
public class HBaseProcess extends ProcessFunction<String, String> {
    private static final Logger logger = LoggerFactory.getLogger(HBaseProcess.class);
    private Connection connection = null;
    private org.apache.hadoop.conf.Configuration configuration = null;
    private Table table = null;
    private String tableName ;

    public void open(Configuration parameters)  {
        this.configuration = HBaseConfiguration.create();

        try {
            configuration.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
            configuration.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));
            this.connection = ConnectionFactory.createConnection(configuration);
        } catch (Exception e) {
            logger.error("HBase Connected failed {}" , e.getMessage());
            e.printStackTrace();
        }
        try {
            this.table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("HBase table is not exit {}." , e.getMessage());
        }

    }

    @Override
    public void close() throws Exception {

        if (null != table ) table.close();
        if (null != connection ) connection.close();
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        // 这里开始对数据做插入
        String[] contents = value.split(",");

        // 设计Rokey , Rokey的设计原则：
        // rowkey设计
        // 1）长度原则
        //      最大值64KB，推荐长度为10 ~ 100byte 主推 16
        //      最好8的倍数，能短则短，rowkey如果太长会影响性能
        // 2）唯一原则 ： rowkey 应该具备唯一性
        // 3）散列原则
        //      3-1）盐值散列：不能使用时间戳直接作为rowkey
        //           在rowkey前增加随机数
        //      3-2）字符串反转 ：1312312334342， 1312312334345
        //           电话号码：133 + 0123 + 4567
        //      3-3) 计算分区号：hashMap
        // 本实例中的rowkey的设计原则为： call1_callName1_call2_callName2
        String rowkey = String.format("%s_%s_%s_%s" , contents[0] , contents[1] ,contents[2] ,contents[3] ) ;
        Put put = new Put(Bytes.toBytes(rowkey));








    }

    class HBaseProcessBuilder {
        private HBaseProcess process ;

        public HBaseProcessBuilder(){
            this.process = new HBaseProcess();
        }
        public HBaseProcessBuilder setTableName(String tableName){
            process.tableName = tableName ;
            return this ;
        }

    }
}
