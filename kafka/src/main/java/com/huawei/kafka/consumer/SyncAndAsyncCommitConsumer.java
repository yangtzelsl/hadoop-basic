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
 * @description:同步结合异步方式提交
 */
public class SyncAndAsyncCommitConsumer {

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

    public SyncAndAsyncCommitConsumer(String topicName) {
        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topicName), new ConsumerRebalanceListener() {

            //Rebalance之前执行
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                //确保在Rebalance的时候也能提交成功
                consumer.commitSync();
            }

            //重新分配分区时执行
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

            }
        });
    }

    //这里使用异步提交和同步提交的组合方式
    public void receiveMsg() {
        try {
            while (isRunning) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                if (!consumerRecords.isEmpty()) {
                    for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                        /*System.out.println("TopicName: " + consumerRecord.topic() + " Partition:" +
                                consumerRecord.partition() + " Offset:" + consumerRecord.offset() + "" +
                                " Msg:" + consumerRecord.value());*/
                        //进行逻辑处理
                        //handle(consumerRecord);
                    }
                    consumer.commitAsync((Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception)-> {
                        //通过回调函数,查看提交结果
                        if (exception != null){
                            consumer.commitSync(offsets);//如果发生异常,尝试重试
                        }
                    });//正常情况使用异步提交,提升性能,因为同步提交需要等待提交结果
                }
            }
        }catch (Exception e){
            //处理异常
        }
        finally {
            //在consumer关闭之前进行同步提交,保证所有offset在程序退出之前提交一次
            consumer.commitSync();
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

