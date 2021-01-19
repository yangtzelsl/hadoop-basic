package com.cyberaray.sql;

import groovy.util.logging.Slf4j;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.mortbay.log.Log;

/**
 *
 */
@Slf4j
public class FlinkSQLKafka2TiDB2 {

    /*

    {
        "id":1238123899121,
        "name":"asdlkjasjkdla998y1122",
        "date":"1990-10-14",
        "obj":{
            "time1":"12:12:43Z",
            "str":"sfasfafs",
            "lg":2324342345
        },
        "arr":[
            {
                "f1":"f1str11",
                "f2":134
            },
            {
                "f1":"f1str22",
                "f2":555
            }
        ],
        "time":"12:12:43Z",
        "timestamp":"1990-10-14T12:12:43Z",
        "map":{
            "flink":123
        },
        "mapinmap":{
            "inner_map":{
                "key":234
            }
        }
    }

    */

    private static final String KAFKA_SOURCE_SQL = "CREATE TABLE news (\n" +
            "    vendor string,\n" +
            "    platform string,\n" +
            "    push_time bigint,\n" +
            "    uuid string,\n" +
            "    table_type string,\n" +
            "    data ROW<site_name string, post_id string, news_url string, gather_time timestamp(3)>\n" +
            ") WITH (\n" +
            "    'connector' = 'kafka',\n" +
            "    'topic' = 'liusilin',\n" +
            "    'scan.startup.mode' = 'earliest-offset',\n" +
            "    'properties.bootstrap.servers' = '172.16.7.52:9092,172.16.7.53:9092,172.16.7.54:9092',\n" +
            "    'properties.security.protocol' = 'SASL_PLAINTEXT',\n" +
            "    'properties.sasl.mechanism' = 'PLAIN',\n" +
            "    'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule  required  username=\"ptreader\"password=\"pt30@123\";',\n" +
            "    'format' = 'json'\n" +
            ")";

    private static final String TiDB_SINK_SQL = "CREATE TABLE news_sink (\n" +
            "    vendor string,\n" +
            "    platform string,\n" +
            "    push_time bigint,\n" +
            "    uuid string,\n" +
            "    table_type string,\n" +
            "    post_id string\n" +
            ") " +
            "WITH (" +
            "'connector' = 'jdbc', " +
            "'url' = 'jdbc:mysql://172.16.7.67:4000/test?useUnicode=true&characterEncoding=utf-8&useSSL=false', " +
            "'username' = 'root', " +
            "'password' = 'root123', " +
            "'table-name' = 'news_sink' " +
            ")";

    public static final String TEMP_VIEW_SQL = "create view temp as SELECT vendor, platform, push_time, uuid, table_type, data.post_id as post_id FROM news ";

    private static final String PROCESS_SQL = "INSERT INTO news_sink SELECT * FROM temp ";


    public static void main(String[] args) throws Exception {

        StreamTableEnvironment streamTableEnv = EnvUtils.initStreamTableEnv();

        String[] temp = new String[]{
                KAFKA_SOURCE_SQL,
                TiDB_SINK_SQL,
                TEMP_VIEW_SQL,
                PROCESS_SQL
        };
        try {
            for (String each : temp) {
                Log.info("Execute sql is :" + each);
                streamTableEnv.executeSql(each);
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
