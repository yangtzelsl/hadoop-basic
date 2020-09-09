package com.cyberaray;

import cn.hutool.core.collection.CollectionUtil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.Consumer;

public class CollectionUtilDemo {

    public static void main(String[] args) {

        ArrayList<String> lists = new ArrayList<>();
        lists.add("hello");
        lists.add("hutool");
        lists.add("flink");
        lists.add("java");
        lists.add("world");

        // 遍历方式一：普通for
        for (int i=0; i<lists.size(); i++) {
            System.out.println(lists.get(i));
        }

        // 遍历方式二：增强for
        for (String list : lists) {
            System.out.println(list);
        }

        // 遍历方式三：lambda
        lists.forEach(list -> System.out.println(list));

        // 遍历方式四：函数式接口
        lists.forEach(System.out::println);

        // 遍历方式五：工具类
        CollectionUtil.forEach(lists.iterator(), (list,index)-> System.out.println(list+": "+index));
    }
}
