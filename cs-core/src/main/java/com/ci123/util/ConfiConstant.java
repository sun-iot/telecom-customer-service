package com.ci123.util;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: telecom-customer-service
 * Package: com.ci123.util
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/11/4 11:54
 */
public class ConfiConstant {

    private static Map<String, String> valueMap = new HashMap<String, String>();

    static {
        // 国际化
        ResourceBundle ct = ResourceBundle.getBundle("config");
        Enumeration<String> enums = ct.getKeys();
        while ( enums.hasMoreElements() ) {
            String key = enums.nextElement();
            String value = ct.getString(key);
            valueMap.put(key, value);
        }

    }

    public static String getVal( String key ) {
        return valueMap.get(key);
    }

    public static void main(String[] args) {
        System.out.println(ConfiConstant.getVal("hbase.zkurl"));
    }
}
