package com.cyberaray.sql;

import groovy.util.logging.Slf4j;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.mortbay.log.Log;

/**
 *
 */
@Slf4j
public class FlinkSQLKafka2TiDB3 {

    /*

    {
        "id":1238123899121,
        "name":"asdlkjasjkdla998y1122",
        "date1":"1990-10-14",
        "obj":{
            "time1":"12:12:43Z",
            "str1":"sfasfafs",
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

    private static final String KAFKA_SOURCE_SQL = "CREATE TABLE json_source (\n" +
            "    id            BIGINT,\n" +
            "    name          STRING,\n" +
            "    `date1`        DATE,\n" +
            "    obj           ROW<time1 TIME,str1 STRING,lg BIGINT>,\n" +
            "    arr           ARRAY<ROW<f1 STRING,f2 INT>>,\n" +
            "    `time`        TIME,\n" +
            "    `timestamp`   TIMESTAMP(3),\n" +
            "    `map`         MAP<STRING,BIGINT>,\n" +
            "    mapinmap      MAP<STRING,MAP<STRING,INT>>,\n" +
            "    proctime as PROCTIME()\n" +
            " ) WITH (\n" +
            "    'connector' = 'kafka',\n" +
            "    'topic' = 'liusilin',\n" +
            "    'scan.startup.mode' = 'latest-offset',\n" +
            "    'properties.bootstrap.servers' = '172.16.7.52:9092,172.16.7.53:9092,172.16.7.54:9092',\n" +
            "    'properties.security.protocol' = 'SASL_PLAINTEXT',\n" +
            "    'properties.sasl.mechanism' = 'PLAIN',\n" +
            "    'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule  required  username=\"ptreader\"password=\"pt30@123\";',\n" +
            "    'format' = 'json'\n" +
            ")";

    private static final String TiDB_SINK_SQL = "CREATE TABLE news_sink (\n" +
            "    id BIGINT,\n" +
            "    name string,\n" +
            "    `date1` DATE,\n" +
            "    str1 string,\n" +
            "    f1 string,\n" +
            "    flink string,\n" +
            "    key string\n" +
            ") " +
            "WITH (" +
            "'connector' = 'jdbc', " +
            "'url' = 'jdbc:mysql://172.16.7.67:4000/test?useUnicode=true&characterEncoding=utf-8&useSSL=false', " +
            "'username' = 'root', " +
            "'password' = 'root123', " +
            "'table-name' = 't_news_sink' " +
            ")";

    //注意数组index从1开始
//    public static final String TEMP_VIEW_SQL = "create view temp as select id, name, date1, obj.str1, arr[1].f1, `map`[\\\"flink\\\"], mapinmap['inner_map']['key'] from json_source ";
    public static final String TEMP_VIEW_SQL = "create view temp as select id, name, date1, obj.str1, arr[1].f1, 'a1' as flink, 'a2' as key  from json_source ";

    // insert into json_source select 111 as id,'name' as name,Row(CURRENT_TIME,'ss',123) as obj,Array[Row('f',1),Row('s',2)] as arr,Map['k1','v1','k2','v2'] as `map`,Map['inner_map',Map['k','v']] as mapinmap;
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
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
