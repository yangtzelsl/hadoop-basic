package com.cyberaray.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class PvUv {

    /**
     * 数据库RDS结果表
     * 字段名	        数据类型	详情
     * summary_date	BIGINT	    统计日期。
     * summary_min	V   ARCHAR	    统计分钟。
     * pv	            BIGINT	    单击量。
     * uv	            BIGINT	    访客量。
     * currenttime	    TIMESTAMP	当前时间。
     */


    public static final String KAFKA_SOURCE_SQL = "CREATE TABLE source_ods_fact_log_track_action (\n" +
            "  account_id VARCHAR,\n" +
            "  --用户ID\n" +
            "  client_ip VARCHAR,\n" +
            "  --客户端IP\n" +
            "  client_info VARCHAR,\n" +
            "  --设备机型信息\n" +
            "  `action` VARCHAR,\n" +
            "  --页面跳转描述\n" +
            "  gpm VARCHAR,\n" +
            "  --埋点链路\n" +
            "  c_time BIGINT,\n" +
            "  --请求时间\n" +
            "  udata VARCHAR,\n" +
            "  --扩展信息，JSON格式\n" +
            "  `position` VARCHAR,\n" +
            "  --位置信息\n" +
            "  network VARCHAR,\n" +
            "  --网络使用情况\n" +
            "  p_dt VARCHAR\n" +
            "  --时间分区天\n" +
            ") WITH (\n" +
            "\t'connector.type' = 'kafka',\n" +
            "\t'connector.version' = 'universal',\n" +
            "\t'connector.topic' = 'topic_uv',\n" +
            "\t'update-mode' = 'append',\n" +
            "\t'connector.properties.zookeeper.connect' = '172.24.103.8:2181',\n" +
            "\t'connector.properties.bootstrap.servers' = '172.24.103.8:9092',\n" +
            "\t'connector.startup-mode' = 'latest-offset',\n" +
            "\t'format.type' = 'json'\n" +
            ")";
    public static final String MYSQL_SINK_SQL = "CREATE TABLE result_cps_total_summary_pvuv_min (\n" +
            "  summary_date varchar,\n" +
            "  --统计日期\n" +
            "  summary_min varchar,\n" +
            "  --统计分钟\n" +
            "  pv bigint,\n" +
            "  --点击量\n" +
            "  uv bigint,\n" +
            "  --一天内同个访客多次访问仅计算一个UV\n" +
            "  current_times varchar\n" +
            "  --当前时间\n" +
            " -- primary key (summary_date, summary_min)\n" +
            ") WITH (\n" +
            "\t'connector.type' = 'jdbc',\n" +
            "\t'connector.url' = 'jdbc:mysql://172.24.103.3:3306/flink',\n" +
            "\t'connector.table' = 'ali_pvuv',\n" +
            "\t'connector.username' = 'root',\n" +
            "\t'connector.password' = '123456',\n" +
            "\t'connector.write.flush.max-rows' = '10',\n" +
            "\t'connector.write.flush.interval' = '5s'\n" +
            ")";
    public static final String TEMP_VIEW_SQL = "select\n" +
            " p_dt  as summary_date, --时间分区\n" +
            " count (client_ip) as pv, --客户端的IP\n" +
            " count (distinct client_ip) as uv, --客户端去重\n" +
            " cast (max (FROM_UNIXTIME(c_time)) as TIMESTAMP) as c_time --请求的时间\n" +
            "from\n" +
            "  source_ods_fact_log_track_action\n" +
            "group\n" +
            "  by p_dt";
    public static final String INSERT_SQL = "INSERT\n" +
            "  into result_cps_total_summary_pvuv_min\n" +
            "select\n" +
            "  a.summary_date,\n" +
            "  --时间分区\n" +
            "  cast (DATE_FORMAT (c_time, 'HH:mm') as varchar) as summary_min,\n" +
            "  --取出小时分钟级别的时间\n" +
            "  a.pv,\n" +
            "  a.uv,\n" +
            "  cast (LOCALTIMESTAMP as varchar) as current_times --当前时间\n" +
            "from\n" +
            "  result_cps_total_summary_pvuv_min_01 AS a";

    public static void main(String[] args) throws Exception {

        //获取流式环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //选用blink
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        //创建流式表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);


        /*--***********************建源表 结果表*******************--*/
        tEnv.sqlUpdate(KAFKA_SOURCE_SQL);
        tEnv.sqlUpdate(MYSQL_SINK_SQL);

        /*--***********************处理逻辑*******************--*/
        //创建视图 按天求出pv uv
        Table table = tEnv.sqlQuery(TEMP_VIEW_SQL);
        tEnv.createTemporaryView("result_cps_total_summary_pvuv_min_01", table);

        tEnv.toRetractStream(table, Row.class).print();
        //数据落地 按分钟统计 pv uv
        tEnv.sqlUpdate(INSERT_SQL);

        tEnv.execute("pvuv");

    }
}
