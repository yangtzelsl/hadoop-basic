package com.cyberaray.sql;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * # 先在MySQL中建表
 * DROP TABLE IF EXISTS `user_behavior`;
 * <p>
 * CREATE TABLE IF NOT EXISTS `user_behavior` (
 * `user_id` bigint(20) DEFAULT NULL,
 * `item_id` bigint(20) DEFAULT NULL
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
 * <p>
 * # KAFKA_HOME
 * /opt/cloudera/parcels/CDH-6.2.1-1.cdh6.2.1.p0.1580995/lib/kafka/bin
 * <p>
 * # 创建topic
 * ./kafka-topics.sh --zookeeper cdh01:2181,cdh02:2181,cdh03:2181 --create --replication-factor 1 --partitions 1 --topic user_behavior
 * <p>
 * # 生产消息
 * ./kafka-console-producer.sh --broker-list 172.16.7.63:9092 --topic user_behavior
 * {"user_id":1,"item_id":1,"category_id":1,"behavior":"pay","ts":1595902335}
 * {"user_id":2,"item_id":2,"category_id":2,"behavior":"pay","ts":1595902335}
 * <p>
 * # 运行程序
 * <p>
 * # 查询数据库验证
 * SELECT * FROM user_behavior ;
 */
public class FlinkSQLKafka2TiDB {

    private static final String KAFKA_SOURCE_SQL = "CREATE TABLE user_behavior (\n" +
            "    user_id BIGINT,\n" +
            "    item_id BIGINT,\n" +
            "    category_id BIGINT,\n" +
            "    behavior STRING,\n" +
            "    ts BIGINT\n" +
            ") WITH (\n" +
            "    'connector.type' = 'kafka',\n" +
            "    'connector.version' = 'universal',\n" +
            "    'connector.topic' = 'user_behavior',\n" +
            "    'connector.startup-mode' = 'latest-offset',\n" +
            "    'connector.properties.bootstrap.servers' = '172.16.7.63:9092',\n" +
            "    'format.type' = 'json'\n" +
            ")";

    private static final String TiDB_SINK_SQL = "CREATE TABLE user_behavior_sink (\n" +
            "    user_id BIGINT,\n" +
            "    counts BIGINT, \n" +
            "    PRIMARY KEY (user_id) NOT ENFORCED\n" +
            ") " +
            "WITH (" +
            "'connector' = 'jdbc', " +
            "'url' = 'jdbc:mysql://172.16.7.80:4000/test?useUnicode=true&characterEncoding=utf-8&useSSL=false', " +
            "'username' = 'root', " +
            "'password' = 'DxoSFdT2cq9MwQR8', " +
            "'table-name' = 'user_behavior_count' " +
            ")";

    //提取读取到的数据，然后只要两个字段，重新发送到 Kafka 新 topic
    private static final String PROCESS_SQL =
            "INSERT INTO user_behavior_sink " +
                    "SELECT user_id, count(item_id) AS counts " +
                    "FROM user_behavior " +
                    "GROUP BY user_id " ;
                    //"ON DUPLICATE KEY UPDATE user_id=user_id";

    public static void main(String[] args) throws Exception {

        StreamTableEnvironment streamTableEnv = EnvUtils.initStreamTableEnv();

        System.out.println(KAFKA_SOURCE_SQL);
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++");
        System.out.println(TiDB_SINK_SQL);

        streamTableEnv.executeSql(KAFKA_SOURCE_SQL);

        streamTableEnv.executeSql(TiDB_SINK_SQL);

        streamTableEnv.executeSql(PROCESS_SQL);

        streamTableEnv.execute("FlinkSQLKafka2TiDB");
    }
}
