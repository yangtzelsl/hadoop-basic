package com.cyberaray;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.UUID;
public class ProductKafkaTransactionnOnly {
    public static void main(String[] args) {
        //创建生产者
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "Centos:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //优化参数
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 1024);//生产者尝试缓存记录，为每一个分区缓存一个mb的数据
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 500);//最多等待0.5秒.
        //开启幂等性 acks必须是-1
        properties.put(ProducerConfig.ACKS_CONFIG,"-1");
        //允许超时最大时间
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,5000);
        //失败尝试次数
        properties.put(ProducerConfig.RETRIES_CONFIG,3);
        //开幂等性  精准一次写入
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);
        //开启事务
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"transaction-id"+ UUID.randomUUID());
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        //初始化事务
        kafkaProducer.initTransactions();
        try {
            //开启事务
            kafkaProducer.beginTransaction();
            for (int i=0;i<5;i++){
                ProducerRecord<String, String> record = new ProducerRecord<>(
                        "topic01",
                        "Transaction",
                        "Test committed  Transaction1");
                kafkaProducer.send(record);
                kafkaProducer.flush();
                if (i==3){
                    Integer b=i/0;
                }
            }
            //事务提交
            kafkaProducer.commitTransaction();
        } catch (ProducerFencedException e) {
            //终止事务
            kafkaProducer.abortTransaction();
            e.printStackTrace();
        }
        kafkaProducer.close();
    }
}


