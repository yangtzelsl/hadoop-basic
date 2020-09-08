package com.cyberaray.stream;

import com.cyberaray.sql.EnvUtils;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.math.BigInteger;

import static org.apache.flink.table.api.Expressions.$;

public class StreamApiKafka2MySQL {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // blink sql 环境初始化
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env, bsSettings);

        DataStream<Row> stream = env.addSource(new RichParallelSourceFunction<Row>() {

                                                   @Override
                                                   public void run(SourceContext<Row> sourceContext) throws Exception {
                                                       Row temp = new Row(1);
                                                       int i = 10;
                                                       while (i > 0) {
                                                           temp.setField(0, "aaa");
                                                           sourceContext.collect(temp);
                                                           i--;
                                                       }
                                                   }

                                                   @Override
                                                   public void cancel() {
                                                       return;
                                                   }
                                               }
        ).setParallelism(1).returns(Types.ROW_NAMED(new String[]{"user"}, new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO}));

        bsTableEnv.createTemporaryView("temp", stream, $("user"));

        // count()等聚合操作属于Retract回退流
        //Table counts = bsTableEnv.sqlQuery("select count(*) from temp");
        // 普通查询属于追加流
        Table temp = bsTableEnv.sqlQuery("select * from temp");
        System.out.println(temp.explain());

        // 表内部格式， 用于debug
        temp.printSchema();

        // 已知坑, table 格式是bigint type内部必须声明为Long ,不要转 Types.BIG_INT 会报类型错误
        TupleTypeInfo<Tuple1<String>> tupleType = new TupleTypeInfo<>(
                Types.STRING
        );

        // 将Sql 转回来 stream 流支持所有类型的数据输出
        //DataStream retractStream = bsTableEnv.toRetractStream(counts, tupleType);
        DataStream appendStream = bsTableEnv.toAppendStream(temp, tupleType);

        appendStream.print().setParallelism(1);


        env.execute("StreamApiKafka2MySQL");
    }
}
