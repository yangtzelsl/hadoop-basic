package com.cyberaray.stream

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object FlinkSQLKafka2Mysql {

  def main(args: Array[String]): Unit = {

    // 获取环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.setParallelism(1)

    val tableEnvSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(streamEnv, tableEnvSettings)
    //tableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
    //tableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(20))


    val source_sql=
      """
        |CREATE TABLE t1 (
        | vendor STRING,
        | push_time BIGINT,
        | data ROW<site_name STRING, gather_time STRING, publish_time STRING>,
        | dtEventTime AS TO_TIMESTAMP(FROM_UNIXTIME(push_time)),
        | WATERMARK FOR dtEventTime AS dtEventTime - INTERVAL '1' MINUTE
        | )
        |    WITH (
        |    'connector' = 'kafka',
        |    'topic' = 'news',
        |    'properties.bootstrap.servers' = '172.16.7.52:9092',
        |    'properties.group.id' = 'g14',
        |    'properties.security.protocol'='SASL_PLAINTEXT',
        |    'properties.sasl.mechanism'='PLAIN',
        |    'properties.sasl.jaas.config'='org.apache.kafka.common.security.plain.PlainLoginModule required username="ptreader" password="pt30@123";',
        |    'format' = 'json',
        |    'scan.startup.mode' = 'earliest-offset',
        |    'json.fail-on-missing-field' = 'false',
        |    'json.ignore-parse-errors' = 'true'
        |    )
      """.stripMargin

    val sink_sql =
      """
        |CREATE TABLE t_out (
        |  `start_push_time` TIMESTAMP(3),
        |  `vendor` STRING,
        |  `site_name` STRING,
        |  `count` BIGINT,
        |  `gather_delay` BIGINT,
        |  `report_delay` BIGINT,
        |  `push_delay` BIGINT
        |  )
        |      WITH (
        |    'connector' = 'jdbc',
        |    'url' = 'jdbc:mysql://172.16.7.60:3307/flink_test?useUnicode=true&characterEncoding=utf-8&useSSL=false',
        |    'username' = 'root',
        |    'password' = '123456',
        |    'table-name' = 'news_out'
        |  )
      """.stripMargin


    val process_sql=
      """
        |INSERT INTO t_out SELECT TUMBLE_START(dtEventTime, INTERVAL '1' MINUTE) as start_push_time, vendor, data.site_name as site_name, COUNT(1) AS `count`, AVG(UNIX_TIMESTAMP(data.gather_time)-UNIX_TIMESTAMP(data.publish_time)) AS gather_delay, AVG(push_time-UNIX_TIMESTAMP(data.gather_time)) AS report_delay, AVG(UNIX_TIMESTAMP()-push_time) AS push_delay FROM t1 GROUP BY vendor, data.site_name, TUMBLE(dtEventTime, INTERVAL '1' MINUTE)
      """.stripMargin

    tableEnv.executeSql(source_sql)

    tableEnv.executeSql(sink_sql)

    tableEnv.executeSql(process_sql)

    streamEnv.execute("FlinkSQLKafka2Mysql Job")

  }

}
