package com.cyberaray.sql;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class JavaFlinkSQLApp {

    // kafka -> print
    private static final String KAFKA_SOURCE_SQL = "CREATE TABLE t1 " +
            "(vendor STRING, push_time BIGINT, data ROW<site_name STRING, gather_time STRING, publish_time STRING>, dtEventTime AS TO_TIMESTAMP(FROM_UNIXTIME(push_time)), WATERMARK FOR dtEventTime AS dtEventTime - INTERVAL '1' MINUTE) " +
            "WITH ( " +
            "'connector' = 'kafka', " +
            "'topic' = 'news', " +
            "'properties.bootstrap.servers' = '172.16.7.52:9092', " +
            "'properties.group.id' = 'g14', " +
            "'properties.security.protocol'='SASL_PLAINTEXT', " +
            "'properties.sasl.mechanism'='PLAIN', " +
            "'properties.sasl.jaas.config'='org.apache.kafka.common.security.plain.PlainLoginModule required username=\"ptreader\" password=\"pt30@123\";', " +
            "'format' = 'json', " +
            "'scan.startup.mode' = 'earliest-offset', " +
            "'json.fail-on-missing-field' = 'false', " +
            "'json.ignore-parse-errors' = 'true' " +
            ")";

    // flink 不推荐直接print的方式输出，如果想看结果需要定义table
    private static final String MYSQL_SINK_SQL = "CREATE TABLE t_out " +
            "(`start_push_time` TIMESTAMP(3), `vendor` STRING, `site_name` STRING, `count` BIGINT, `gather_delay` BIGINT, `report_delay` BIGINT, `push_delay` BIGINT) " +
            "WITH (" +
            "'connector' = 'jdbc', " +
            "'url' = 'jdbc:mysql://172.16.7.60:3307/flink_test?useUnicode=true&characterEncoding=utf-8&useSSL=false', " +
            "'username' = 'root', " +
            "'password' = '123456', " +
            "'table-name' = 'news_out_2' " +
            ")";

    private static final String PROCESS_SQL = "INSERT INTO t_out SELECT TUMBLE_START(dtEventTime, INTERVAL '1' MINUTE) as start_push_time, vendor, data.site_name as site_name, COUNT(1) AS `count`, AVG(UNIX_TIMESTAMP(data.gather_time)-UNIX_TIMESTAMP(data.publish_time)) AS gather_delay, AVG(push_time-UNIX_TIMESTAMP(data.gather_time)) AS report_delay, AVG(UNIX_TIMESTAMP()-push_time) AS push_delay FROM t1 GROUP BY vendor, data.site_name, TUMBLE(dtEventTime, INTERVAL '1' MINUTE)";

    
    public static void main(String[] args) throws Exception {

        StreamTableEnvironment streamTableEnv = EnvUtils.initStreamTableEnv();

        System.out.println(KAFKA_SOURCE_SQL);
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++");
        System.out.println(MYSQL_SINK_SQL);

        streamTableEnv.executeSql(KAFKA_SOURCE_SQL);

        streamTableEnv.executeSql(MYSQL_SINK_SQL);

        streamTableEnv.executeSql(PROCESS_SQL);

        streamTableEnv.execute("JavaFlinkSQLApp");
    }
}
