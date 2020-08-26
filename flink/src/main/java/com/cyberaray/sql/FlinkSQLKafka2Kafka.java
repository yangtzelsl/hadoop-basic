package com.cyberaray.sql;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * # KAFKA_HOME
 * /opt/cloudera/parcels/CDH-6.2.1-1.cdh6.2.1.p0.1580995/lib/kafka/bin
 *
 * # 创建topic
 * ./kafka-topics.sh --zookeeper cdh01:2181,cdh02:2181,cdh03:2181 --create --replication-factor 1 --partitions 1 --topic user_behavior
 * ./kafka-topics.sh --zookeeper cdh01:2181,cdh02:2181,cdh03:2181 --create --replication-factor 1 --partitions 1 --topic user_behavior_sink
 *
 * # 生产消息
 * ./kafka-console-producer.sh --broker-list 172.16.7.63:9092 --topic user_behavior
 * {"user_id":1,"item_id":1,"category_id":1,"behavior":"pay","ts":1595902335}
 * {"user_id":2,"item_id":2,"category_id":2,"behavior":"pay","ts":1595902335}
 *
 * # 运行程序
 *
 * # 消费验证
 * ./kafka-console-consumer.sh --bootstrap-server 172.16.7.63:9092,172.16.7.64:9092,172.16.7.65:9092 --from-beginning --topic user_behavior_sink
 */
public class FlinkSQLKafka2Kafka {

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

    private static final String KAFKA_SINK_SQL = "CREATE TABLE user_behavior_sink (\n" +
            "    user_id BIGINT,\n" +
            "    item_id BIGINT\n" +
            ") WITH (\n" +
            "    'connector.type' = 'kafka',\n" +
            "    'connector.version' = 'universal',\n" +
            "    'connector.topic' = 'user_behavior_sink',\n" +
            "    'connector.properties.bootstrap.servers' = '172.16.7.63:9092',\n" +
            "    'format.type' = 'json',\n" +
            "    'update-mode' = 'append'\n" +
            ")";

    //提取读取到的数据，然后只要两个字段，重新发送到 Kafka 新 topic
    private static final String PROCESS_SQL = "insert into user_behavior_sink select user_id, item_id from user_behavior";

    public static void main(String[] args) throws Exception {

        StreamTableEnvironment streamTableEnv = EnvUtils.initStreamTableEnv();

        System.out.println(KAFKA_SOURCE_SQL);
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++");
        System.out.println(KAFKA_SINK_SQL);

        streamTableEnv.executeSql(KAFKA_SOURCE_SQL);

        streamTableEnv.executeSql(KAFKA_SINK_SQL);

        streamTableEnv.executeSql(PROCESS_SQL);

        streamTableEnv.execute("FlinkSQLKafka2Kafka");
    }
}
