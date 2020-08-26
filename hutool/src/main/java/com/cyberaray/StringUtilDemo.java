package com.cyberaray;

import cn.hutool.core.util.StrUtil;

public class StringUtilDemo {
    public static void main(String[] args) {

        // 1.空与非空的操作
        // 是否为空白，空白的定义是null,"",不可见字符（如空格）
        System.out.println(StrUtil.isBlank(""));
        System.out.println(StrUtil.isNotBlank(""));
        // 字符串列表是否有空白字符串
        System.out.println(StrUtil.hasBlank(""));
        // 给定字符串列表是否全为空白
        System.out.println(StrUtil.isAllBlank(""));

        // 是否为空，空的定义是null,""
        System.out.println(StrUtil.isEmpty(""));
        System.out.println(StrUtil.isNotEmpty(""));
        System.out.println(StrUtil.hasEmpty(""));
        System.out.println(StrUtil.isAllEmpty(""));
        // 给定字符串为空时返回""
        System.out.println(StrUtil.nullToEmpty(""));
        System.out.println(StrUtil.emptyToNull(""));
        // 给定字符串为空null时返回默认字符串，否则返回本身
        System.out.println(StrUtil.nullToDefault("",""));
        System.out.println(StrUtil.nullToEmpty(""));

        // 2.指定字符开头或结尾
        // 是否以指定字符或者指定字符串开头
        System.out.println(StrUtil.startWith("", ""));
        // 忽略大小写
        System.out.println(StrUtil.startWithIgnoreCase("", ""));
        // 以任意字符串开始
        System.out.println(StrUtil.startWithAny("", ""));

        System.out.println(StrUtil.endWith("", ""));
        System.out.println(StrUtil.endWithIgnoreCase("", ""));
        System.out.println(StrUtil.endWithAny("", ""));

        // 3.去掉指定前后缀
        System.out.println(StrUtil.removePrefix("", ""));
        System.out.println(StrUtil.removeSuffix("", ""));

        // 4.模板操作——类似slf4j的占位符进行字符串拼接
        String template = "{}爱{}，就像老鼠爱大米";
        String str = StrUtil.format(template, "我", "你"); //str -> 我爱你，就像老鼠爱大米
        System.out.println(str);
    }
}
