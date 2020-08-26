package com.cyberaray.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class StreamApiKafka2HDFS {

    public static void checkPointIni(StreamExecutionEnvironment env) {
        // checkpoint configure
        // start a checkpoint every 3000 ms
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // enable externalized checkpoints which are retained after job cancellation
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // allow job recovery fallback to checkpoint when there is a more recent savepoint
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
    }

    public static DataStream<String> kafkaSourceIni(StreamExecutionEnvironment env) throws Exception {
        Properties pros = new Properties();
        pros.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "cdh01:9092,cdh02:9092,cdh03:9092");
        pros.setProperty("max.poll.records", "50");
        pros.setProperty("auto.offset.reset", "earliest");
        pros.put("group.id", "test");

        DataStream<String> input = env.addSource(new FlinkKafkaConsumer<>("user_behavior",
                        new SimpleStringSchema(), pros)
                , "test-state")
                .setParallelism(1);
        return input;
    }

    public static DataStream<String> kafkaSinkIni(DataStream<String>  input) throws Exception {
        Properties pros = new Properties();
        pros.setProperty("bootstrap.servers", "cdh01:9092,cdh02:9092,cdh03:9092");
        pros.setProperty("transaction.timeout.ms", "30000");
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
                "sink-test",
                new KeyedSerializationSchemaWrapper(new SimpleStringSchema()),
                pros,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // fault-tolerance

        input.addSink(myProducer);
        return input;
    }

    public static DataStreamSink<String> hdfsSinkIni(DataStream<String>  input, String path) throws Exception {

        DataStreamSink<String> hdfsSink = input.addSink(new BucketingSink<>(path)).setParallelism(1);

        return hdfsSink;
    }

    public static DataStreamSink<String> fileSystemSinkIni(DataStream<String>  input, String path) throws Exception {

        DataStreamSink<String> fsSink = input.writeAsText(path, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        return fsSink;
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        checkPointIni(env);

        String filePath = "/data/show";

        String hdfsFilePath = "hdfs://k8s02:8020/user/flink/behavior";

        DataStream<String> input = kafkaSourceIni(env);
        input.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                Thread.sleep(1);
                return s;
            }
        }).setParallelism(1);

        //kafkaSinkIni(input);

        hdfsSinkIni(input, hdfsFilePath);

        //fileSystemSinkIni(input, filePath);

        env.execute("StreamApiKafka2HDFS");
    }
}
