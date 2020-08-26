package com.cyberaray.table;

import com.cyberaray.sql.EnvUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

public class TableApiKafka2Kafka {

    public static void main(String[] args) throws Exception {

        StreamTableEnvironment streamTableEnv = EnvUtils.initStreamTableEnv();

        streamTableEnv.connect(new Kafka()
                .version("universal") //定义版本
                .topic("user_behavior") //定义主题
                .property("zookeeper.connect", "172.16.7.63:2181") // zookeeper
                .property("bootstrap.servers", "172.16.7.63:9090") // bootstrap.server
                .startFromEarliest()
        )
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("user_id", DataTypes.INT())
                        .field("item_id", DataTypes.INT())
                        .field("category_id", DataTypes.INT())
                        .field("behavior", DataTypes.STRING())
                        .field("ts", DataTypes.INT()))
                .createTemporaryTable("kafkaInputTable");

        Table kafkaInputTable = streamTableEnv.from("kafkaInputTable");
                //.select($("user_id"), $("item_id"))
                //.filter($("user_id").isEqual(2));
        //.groupBy("id")

        streamTableEnv.toAppendStream(kafkaInputTable, KafkaInputTable.class).print();

        streamTableEnv.connect(new Kafka()
                .version("universal") //定义版本
                .topic("user_behavior_sink") //定义主题
                .property("zookeeper.connect", "172.16.7.63:2181") // zookeeper
                .property("bootstrap.servers", "172.16.7.63:9090") // bootstrap.server
        )
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("user_id", DataTypes.BIGINT())
                        .field("item_id", DataTypes.BIGINT()))
                .createTemporaryTable("kafkaOutputTable");

        kafkaInputTable.insertInto("kafkaOutputTable");


        streamTableEnv.execute("TableApiKafka2Kafka");
    }

    public static class KafkaInputTable {
        private Integer user_id;
        private Integer item_id;
        private Integer category_id;
        private String behavior;
        private Integer ts;

        @Override
        public String toString() {
            return "KafkaInputTable{" +
                    "user_id=" + user_id +
                    ", item_id=" + item_id +
                    ", category_id=" + category_id +
                    ", behavior='" + behavior + '\'' +
                    ", ts=" + ts +
                    '}';
        }

        public Integer getUser_id() {
            return user_id;
        }

        public void setUser_id(Integer user_id) {
            this.user_id = user_id;
        }

        public Integer getItem_id() {
            return item_id;
        }

        public void setItem_id(Integer item_id) {
            this.item_id = item_id;
        }

        public Integer getCategory_id() {
            return category_id;
        }

        public void setCategory_id(Integer category_id) {
            this.category_id = category_id;
        }

        public String getBehavior() {
            return behavior;
        }

        public void setBehavior(String behavior) {
            this.behavior = behavior;
        }

        public Integer getTs() {
            return ts;
        }

        public void setTs(Integer ts) {
            this.ts = ts;
        }
    }
}
