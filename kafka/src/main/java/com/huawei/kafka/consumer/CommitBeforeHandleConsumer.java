package com.huawei.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

/**
 * @author: xuqiangnj@163.com
 * @date: 2019/5/3 13:13
 * @description:异步方式提交
 */
public class CommitBeforeHandleConsumer {

    private static Properties props = new Properties();
    static {
        props.put("bootstrap.servers", "192.168.142.139:9092");
        props.put("group.id", "Test");
        props.put("enable.auto.commit", false);//注意这里设置为手动提交方式
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    final KafkaConsumer<String, String> consumer;

    private volatile boolean isRunning = true;

    public CommitBeforeHandleConsumer(String topicName) {
        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topicName));
    }

    //这里使用异步提交和同步提交的组合方式
    public void receiveMsg() {
        try {
            while (isRunning) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                if (!consumerRecords.isEmpty()) {
                    consumer.commitAsync();//在业务处理前提交offset
                    for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                        /*System.out.println("TopicName: " + consumerRecord.topic() + " Partition:" +
                                consumerRecord.partition() + " Offset:" + consumerRecord.offset() + "" +
                                " Msg:" + consumerRecord.value());*/
                        //进行逻辑处理
                        //handle(consumerRecord);
                    }
                }
            }
        }catch (Exception e){
            //处理异常
        }
        finally {
            close();
        }
    }

    public void close() {
        isRunning = false;
        if (consumer != null) {
            consumer.close();
        }
    }
}
