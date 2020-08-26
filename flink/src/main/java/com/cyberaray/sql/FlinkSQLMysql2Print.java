package com.cyberaray.sql;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQLMysql2Print {

    private static final String MYSQL_SOURCE_SQL = "CREATE TABLE t_out " +
            "(`vendor` STRING, `report_delay` BIGINT) " +
            "WITH (" +
            "'connector' = 'jdbc', " +
            "'url' = 'jdbc:mysql://172.16.7.60:3307/flink_test?useUnicode=true&characterEncoding=utf-8&useSSL=false', " +
            "'username' = 'root', " +
            "'password' = '123456', " +
            "'table-name' = 'news_out' " +
            ")";

    // flink 不推荐直接print的方式输出，如果想看结果需要定义table
    private static final String PRINT_SINK_SQL = "create table sink_print ( \n" +
            " vendor STRING," +
            " report_delay BIGINT " +
            ") with ('connector' = 'print' )";

    private static final String MYSQL_PROCESS_SQL = "INSERT INTO sink_print SELECT * FROM t_out ";

    public static void main(String[] args) throws Exception {

        StreamTableEnvironment streamTableEnv = EnvUtils.initStreamTableEnv();

        streamTableEnv.executeSql(MYSQL_SOURCE_SQL);

        streamTableEnv.executeSql(PRINT_SINK_SQL);

        streamTableEnv.executeSql(MYSQL_PROCESS_SQL);

        streamTableEnv.execute("FlinkSQLMysql2Print");
    }
}
