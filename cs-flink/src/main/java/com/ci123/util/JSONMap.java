package com.ci123.util;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p> 这个类只要用于 JSON 格式和 Map 格式数据之间的转换
 * Project: telecom-customer-service
 * Package: com.ci123.util
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/11/5 9:49
 */
public class JSONMap {
    /**
     * 将一个 类对象转化成一个 JSON 字符串
     * 这里暂不考虑 对象的其它的可能性
     * @param o
     * @return
     */
    public static String objectToJSON(Object o){
        return JSON.toJSONString(o) ;
    }

    /**
     * 将一个对象转换成一个 Map
     * @param o
     * @return
     * @throws IllegalAccessException
     */
    public static Map<String,Object> jsonToMap(Object o) throws IllegalAccessException {
        Class<?> aClass = o.getClass();
        Map<String,Object> map = new HashMap<>();
        while (null != aClass.getSuperclass()) {
            // 得到所有的字段名
            Field[] fields = aClass.getDeclaredFields();
            for (Field field : fields) {
                String filedName = field.getName();
                // 拿到访问的权限
                field.setAccessible(true);
                // 拿到我们的值
                Object o1 = field.get(o);
                // 回复权限控制
                field.setAccessible(true);
                if (null != o1 && StringUtils.isNotBlank(o1.toString())){
                    // 如果是 List ， 则转化为List
                    if (o1 instanceof List){
                        o1 = JSON.toJSONString(o1) ;
                    }
                    map.put(filedName , o1) ;
                }

            }
            aClass = aClass.getSuperclass();
        }
        return map ;
    }

}
