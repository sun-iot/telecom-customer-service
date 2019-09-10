package com.ci123.constant;

import com.ci123.bean.Val;

/**
 * 名称常量枚举类
 */
public enum Names implements Val {
    NAMESPACE("cs")
    ,TABLE("cs:calllog")
    ,CF_CALLER("caller")
    ,CF_CALLEE("callee")
    ,CF_INFO("info")
    ,TOPIC("cs");


    private String name;

    private Names( String name ) {
        this.name = name;
    }


    public void setValue(Object val) {
       this.name = (String)val;
    }

    public String getValue() {
        return name;
    }
}
