package com.ci123.root;

import com.ci123.hbase.HBaseUtil;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: telecom-customer-service
 * Package: com.ci123.root
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/11/1 17:22
 */
public class Main {

    public static void main(String[] args) {
        HBaseUtil hBaseUtil = new HBaseUtil("192.168.1.111", 2181, "192.168.1.111:16000");

        boolean table = hBaseUtil.createTable("user", "info");
        System.out.println(table);

    }
}
