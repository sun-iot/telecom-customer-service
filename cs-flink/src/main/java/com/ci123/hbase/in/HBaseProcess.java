package com.ci123.hbase.in;

import com.ci123.hbase.HBaseUtil;
import com.ci123.util.ConfiConstant;
import com.ci123.util.HBaseConfiguration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
    private String[] qualifiers ;
    private String tableName ;
    public HBaseProcess(String[] qualifiers , String tableName){
        this.qualifiers = qualifiers ;
        this.tableName = tableName ;
    }
    HBaseUtil build = null ;
    public void open(Configuration parameters)  {
        this.build = HBaseUtil.create()
                .setZkUrl(ConfiConstant.getVal(HBaseConfiguration.HBASE_ZK_URL))
                .setZkPort(ConfiConstant.getVal(HBaseConfiguration.HBASE_ZK_PORT))
                .setMasterUrl(ConfiConstant.getVal(HBaseConfiguration.HBASE_MASTER_URL))
                .build();
    }

    @Override
    public void close() throws Exception {
        build.close();
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        // 这里开始对数据做插入
        String[] contents = value.split(",");
        String rowkey = String.format("%s_%s_%s" , contents[0] ,contents[2],contents[5]) ;
        build.putList(tableName , rowkey , "info" , qualifiers, contents) ;
    }

}
