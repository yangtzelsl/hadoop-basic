package com.cyberaray.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
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

    private static final String PRINT_SINK_SQL_2 = "create table sink_print_2 ( \n" +
            " vendor STRING," +
            " counts BIGINT," +
            " uio   STRING " +
            ") with ('connector' = 'print' )";

    private static final String PRINT_PROCESS_SQL = "INSERT INTO sink_print SELECT vendor, count(push_time) as counts FROM t1 group by vendor";
    private static final String PRINT_PROCESS_SQL_2 = "INSERT INTO sink_print_2 SELECT vendor, count(push_time) as counts, 'aaa' as uio  FROM t1 group by vendor";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //StreamTableEnvironment streamTableEnv = EnvUtils.initStreamTableEnv();
//        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        EnvironmentSettings tableEnvSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv,tableEnvSettings);
        streamTableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        StatementSet statementSet = streamTableEnv.createStatementSet();

        System.out.println(KAFKA_SOURCE_SQL);
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++");
        System.out.println(PRINT_SINK_SQL);
        System.out.println(PRINT_SINK_SQL_2);

        streamTableEnv.executeSql(KAFKA_SOURCE_SQL);

        streamTableEnv.executeSql(PRINT_SINK_SQL);
        streamTableEnv.executeSql(PRINT_SINK_SQL_2);

        statementSet.addInsertSql(PRINT_PROCESS_SQL);
        statementSet.addInsertSql(PRINT_PROCESS_SQL_2);

        //streamTableEnv.execute("FlinkSQLKafka2Print");
        statementSet.execute();

    }
}
