package com.ci123.root;

import com.ci123.product.ProductLog;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: telecom-customer-service
 * Package: com.ci123.root
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/7 17:52
 */
public class ProduceMain {
    public static void main(String[] args) {
        ProductLog productLog = new ProductLog() ;
        productLog.init();
        productLog.writeLog("./cs-producer/callLog.csv" , productLog);
    }

}
