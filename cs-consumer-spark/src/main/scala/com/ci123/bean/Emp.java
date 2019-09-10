package com.ci123.bean;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: telecom-customer-service
 * Package: com.ci123.bean
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/10 9:36
 */
public class Emp {
    private String empid ;
    private String name ;
    private Integer age ;
    private String sex ;
    private String company ;
    private String address ;
    private String deptid ;

    public Emp(String empid, String name, Integer age, String sex, String company, String address, String deptid) {
        this.empid = empid;
        this.name = name;
        this.age = age;
        this.sex = sex;
        this.company = company;
        this.address = address;
        this.deptid = deptid;
    }

    public Emp() {
    }

    public String getEmpid() {
        return empid;
    }

    public void setEmpid(String empid) {
        this.empid = empid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getDeptid() {
        return deptid;
    }

    public void setDeptid(String deptid) {
        this.deptid = deptid;
    }

}
