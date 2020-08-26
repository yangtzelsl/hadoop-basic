package com.cyberaray.sql;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQLKafka2Print {

    private static final String KAFKA_SOURCE_SQL = "CREATE TABLE t1 (" +
            "vendor STRING," +
            "push_time BIGINT" +
            ") WITH (" +
            "'connector' = 'kafka'," +
            "'topic' = 'news', " +
            "'properties.bootstrap.servers' = '172.16.7.63:9092'," +
            "'properties.group.id' = 'g1'," +
            "'format' = 'json', " +
            "'scan.startup.mode' = 'earliest-offset', " +
            "'json.fail-on-missing-field' = 'false', " +
            "'json.ignore-parse-errors' = 'true' " +
            ")";

    // flink 不推荐直接print的方式输出，如果想看结果需要定义table
    private static final String PRINT_SINK_SQL = "create table sink_print ( \n" +
            " vendor STRING," +
            " counts BIGINT " +
            ") with ('connector' = 'print' )";

    private static final String PRINT_PROCESS_SQL = "INSERT INTO sink_print SELECT vendor, count(push_time) as counts FROM t1 group by vendor";

    public static void main(String[] args) throws Exception {

        StreamTableEnvironment streamTableEnv = EnvUtils.initStreamTableEnv();

        System.out.println(KAFKA_SOURCE_SQL);
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++");
        System.out.println(PRINT_SINK_SQL);

        streamTableEnv.executeSql(KAFKA_SOURCE_SQL);

        streamTableEnv.executeSql(PRINT_SINK_SQL);

        streamTableEnv.executeSql(PRINT_PROCESS_SQL);

        streamTableEnv.execute("FlinkSQLKafka2Print");
    }
}
