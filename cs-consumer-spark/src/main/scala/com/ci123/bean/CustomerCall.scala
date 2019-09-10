package com.ci123.bean

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved 
 *
 * Project: telecom-customer-service
 * Package: com.ci123.bean
 * Version: 1.0
 *
 * Created by SunYang on 2019/9/7 19:09
 */
// 电信数据的实例类
case class CustomerCall(
                         call1: String,
                         call1_name: String,
                         call2: String,
                         call2_name: String,
                         date_time: String,
                         date_time_ts: String,
                         duration: Long
                       ) {}
