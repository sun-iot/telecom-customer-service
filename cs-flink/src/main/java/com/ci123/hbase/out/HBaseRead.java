package com.ci123.hbase.out;

import com.ci123.hbase.HBaseUtil;
import com.ci123.util.ConfiConstant;
import com.ci123.util.HBaseConfiguration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: telecom-customer-service
 * Package: com.ci123.hbase.out
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/11/4 15:35
 */
public class HBaseRead extends RichSourceFunction<String> {

    private static final Logger logger = LoggerFactory.getLogger(HBaseRead.class);
    HBaseUtil build = null;
    private String tableName ;
    public HBaseRead(String tableName){
        this.tableName = tableName ;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.build = HBaseUtil.create()
                .setZkUrl(ConfiConstant.getVal(HBaseConfiguration.HBASE_ZK_URL))
                .setZkPort(ConfiConstant.getVal(HBaseConfiguration.HBASE_ZK_PORT))
                .setMasterUrl(ConfiConstant.getVal(HBaseConfiguration.HBASE_MASTER_URL))
                .build();
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        List<Result> allRows = build.getAllRows(tableName);
        for (Result result : allRows) {
            String rowKey = Bytes.toString(result.getRow());
            StringBuilder sb = new StringBuilder();
            sb.append(rowKey).append(",");
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                sb.append(value).append("," );
            }
            String resultStr = sb.replace(sb.length() - 1, sb.length(), "").toString();
            ctx.collect(resultStr);
        }
    }

    @Override
    public void cancel() {
        build.close();
    }
}
