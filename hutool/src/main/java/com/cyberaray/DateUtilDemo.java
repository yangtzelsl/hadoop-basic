package com.cyberaray;

import cn.hutool.core.date.DateField;
import cn.hutool.core.date.DateUtil;

public class DateUtilDemo {
    public static void main(String[] args) {

        // 返回当前时间，格式为yyyy-MM-dd HH:mm:ss
        System.out.println(DateUtil.now());

        // 返回当前日期，格式为yyyy-MM-dd
        System.out.println(DateUtil.today());

        // 当前时间的DateTime对象（相当于new DateTime()或者new Date()）
        System.out.println(DateUtil.date());

        // 根据给定的Date对象返回一个年份和季节的字符串
        System.out.println(DateUtil.yearAndQuarter(DateUtil.date()));

        // 日期偏移
        System.out.println(DateUtil.offset(DateUtil.date(), DateField.YEAR, 10));

        // 解析日期字符串
        System.out.println(DateUtil.parse(DateUtil.now()).toString());
    }
}
