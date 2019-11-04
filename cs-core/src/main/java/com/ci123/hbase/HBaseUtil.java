package com.ci123.hbase;

import com.jcraft.jsch.MAC;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p> HBase的操作的工具类
 * Project: telecom-customer-service
 * Package: com.ci123.hbase
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/11/1 15:19
 */
public class HBaseUtil {
    private static final Logger logger = LoggerFactory.getLogger(HBaseUtil.class);
    private static  String zkUrl ;
    private static  Integer zkPort;
    private static  String masterUrl ;
    private static Configuration configuration;
    private static  ExecutorService executor ;
    private static  Connection connection;


    public HBaseUtil(String zkUrl , Integer zkPort , String masterUrl){
        this.zkPort = zkPort ;
        this.zkUrl = zkUrl ;
        this.masterUrl = masterUrl ;
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zkUrl);
        configuration.set("hbase.zookeeper.property.clientPort", String.valueOf(zkPort));
        executor = Executors.newFixedThreadPool(32);
        try {
            connection = ConnectionFactory.createConnection(configuration , executor);
        } catch (IOException e) {
            logger.error("HBase connect failed {}.", e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 判断 HBase中的表是否存在
     * @param tableName
     * @return true
     */
    public boolean isTableExit(String tableName) {
        HBaseAdmin admin = null;
        try {
            admin = (HBaseAdmin)connection.getAdmin();
            return admin.tableExists(tableName);
        } catch (IOException e) {
            logger.error("server connect failed {}", e.getMessage());
            return false;
        }finally {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建HBase表
     *
     * @param tableName
     * @param families
     * @return true
     */
    public boolean createTable(String tableName, String... families) {
        if (isTableExit(tableName)) {
            logger.info("the table is exit , nothing to do.");
            return false;
        } else {
            HBaseAdmin admin = null ;
            // 创建表属性的对象，表名需要转字节
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            // 创建多个列族
            for (String family : families) {
                tableDescriptor.addFamily(new HColumnDescriptor(family));
            }
            // 根据表的配置，创建表
            try {
                admin = (HBaseAdmin)connection.getAdmin();
                admin.createTable(tableDescriptor);
                logger.info("the table create successful");
                return true;
            } catch (IOException e) {
                logger.error("table create failed {}.", e.getMessage());
                return false;
            }finally {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 删除 HBase中的表
     *
     * @param tableName
     * @return true
     */
    public boolean deleteTable(String tableName) {
        if (isTableExit(tableName)) {
            HBaseAdmin admin = null ;
            try {
                admin.disableTable(tableName);
            } catch (IOException e) {
                logger.error("table disable failed {}.", e.getMessage());
                return false;
            }
            try {
                admin.deleteTable(tableName);
            } catch (IOException e) {
                logger.error("table delete failed {].", e.getMessage());
                return false;
            }finally {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return true;
        } else {
            logger.error("table is not exits");
            return false;
        }
    }

    /**
     * 插入一行数据
     *
     * @param tableName
     * @param rowkey
     * @param family
     * @param qualifier
     * @param value
     * @return
     */
    public boolean putRowData(String tableName, String rowkey, String family, String qualifier, String value) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            logger.error("get table failed {}.", e.getMessage());
            return false;
        }
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        try {
            table.put(put);
            return true;
        } catch (IOException e) {
            logger.error("table put failed {}.", e.getMessage());
            return false;
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                logger.error("table close failed {}.", e.getMessage());
                return false;
            }
        }
    }

    /**
     * 删除多行数据
     *
     * @param tableName
     * @param rowkeys
     * @return
     */
    public boolean deleteMultiRow(String tableName, String... rowkeys) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            logger.error("get table failed {}.", e.getMessage());
            return false;
        }
        List<Delete> deleteList = new ArrayList<Delete>();
        for (String rowkey : rowkeys) {
            deleteList.add(new Delete(Bytes.toBytes(rowkey)));
        }
        try {
            table.delete(deleteList);
            return true;
        } catch (IOException e) {
            logger.error("delete multi rowkey failed {}.", e.getMessage());
            return false;
        }
    }

    /**
     * 拿到左右的数据
     *
     * @param tableName
     * @return
     */
    public List<Result> getAllRows(String tableName) {
        Table table = null;
        Scan scan = new Scan();
        List<Result> resultList = null;
        table = getTable(tableName);
        try {
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                resultList.add(result);
            }
            return resultList;
        } catch (IOException e) {
            logger.error("scan failed {}.", e.getMessage());
            return null;
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                logger.error("table close failed");
            }
        }
    }

    /**
     * 获取某一行的数据
     *
     * @param tableName
     * @param rowkey
     * @param showVersion 是否显示当前的版本信息
     * @return
     */
    public Result getRow(String tableName, String rowkey, boolean showVersion) {
        Table table = null;
        Get get = new Get(Bytes.toBytes(rowkey));
        if (showVersion) {
            get.setMaxVersions();
        }
        table = getTable(tableName);
        try {
            return table.get(get);
        } catch (IOException e) {
            logger.error("get failed {}.", e.getMessage());
            return null;
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                logger.error("table close failed");
            }
        }
    }

    /**
     * 根据当前的时间戳，显示某一行的所有的细腻，包括版本信息
     *
     * @param tableName
     * @param rowkey
     * @param timestamp 时间戳，显示指定时间戳的版本
     * @return
     */
    public Result getRow(String tableName, String rowkey, long timestamp) {
        Table table = null;
        Get get = new Get(Bytes.toBytes(rowkey));
        get.setMaxVersions();
        try {
            get.setTimeStamp(timestamp);
        } catch (IOException e) {
            logger.error("get setTimestamp failed {].", e.getMessage());
            return null;
        }
        table = getTable(tableName);
        try {
            return table.get(get);
        } catch (IOException e) {
            logger.error("get failed {}.", e.getMessage());
            return null;
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                logger.error("table close failed");
            }
        }
    }

    /**
     * 获取某一行指定 “列族：列”的数据
     *
     * @param tableName
     * @param rowkey
     * @param family
     * @param qualifier
     * @return
     */
    public Result getQualifier(String tableName, String rowkey, String family, String qualifier) {
        Table table = null;
        Get get = new Get(Bytes.toBytes(tableName));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        table = getTable(tableName);
        try {
            return table.get(get);
        } catch (IOException e) {
            logger.error("get qualifier failed {}.", e.getMessage());
            return null;
        }
    }

    /**
     * 获取一个HBase的表
     *
     * @param tableName
     * @return
     */
    private Table getTable(String tableName) {
        try {
            return connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            logger.error("get table failed {}.", e.getMessage());
            return null;
        }
    }

}
