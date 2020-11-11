package com.cyberaray.connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Flink2Hive {
    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name = "flink";
        String defaultDatabase = "flink_hive_test";
//        String hiveConfDir = "E:\\IDEA2018\\hadoop-basic\\flink\\src\\main\\resources\\"; // a local path
        String hiveConfDir = "/opt/hive_conf/";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("flink", hive);

        // set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("flink");

        tableEnv.executeSql("insert overwrite test values ('newKey2', 'newVal2')");

        System.out.println("数据插入Hive完成！");

    }
}
