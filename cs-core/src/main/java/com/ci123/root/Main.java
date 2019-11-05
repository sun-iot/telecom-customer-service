package com.ci123.root;

import com.ci123.hbase.HBaseUtil;
import com.ci123.util.ConfiConstant;
import com.ci123.util.HBaseConfiguration;

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
    private String zkUrl ;

    public static void main(String[] args) {

        HBaseUtil build = HBaseUtil.create()
                .setZkUrl(ConfiConstant.getVal(HBaseConfiguration.HBASE_ZK_URL))
                .setZkPort(ConfiConstant.getVal(HBaseConfiguration.HBASE_ZK_PORT))
                .setMasterUrl(ConfiConstant.getVal(HBaseConfiguration.HBASE_MASTER_URL))
                .build();

        System.out.println(build.deleteTable("telecom-customer-service"));

        boolean user = build.createTable("telecom-customer-service" , "info");
        System.out.println(user);

        build.close();
    }
}
