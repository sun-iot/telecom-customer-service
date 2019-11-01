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
        HBaseUtil build = HBaseUtil.create().setZkUrl("192.168.1.111,192.168.1.112.192.168.1.113")
                .setzkPort(2181)
                .build();

        boolean student = build.isTableExit("student");
        System.out.println(student);
    }
}
