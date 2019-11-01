package com.ci123.hbase.in;

import com.ci123.hbase.BaseInput;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
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
 * Created by SunYang on 2019/10/31 17:19
 */
public class HBaseInput implements BaseInput<String> {
    private static final Logger logger = LoggerFactory.getLogger(HBaseInput.class);
    private org.apache.hadoop.conf.Configuration conf = null;
    private Connection conn = null;
    private Table table = null;

    private HBaseInput(){

    }
    public static HBaseInput create(){
        return new HBaseInput();
    }
    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {

    }

    @Override
    public void writeRecord(Object record) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}
