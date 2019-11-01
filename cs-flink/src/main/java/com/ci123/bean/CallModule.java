package com.ci123.bean;

import com.alibaba.fastjson.JSON;
import lombok.Data;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: telecom-customer-service
 * Package: com.ci123.bean
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/10/31 10:22
 */
@Data
public class CallModule {
    public long phoneNumberA ;
    public String nameA ;
    public long phoneNumberB ;
    public String nameB ;
    public String dateTime ;
    public long timestamp ;
    public long duration;

    public String toString(){
        return JSON.toJSONString(this) ;
    }
}
