package com.cyberaray.common;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Arrays;
import java.util.Properties;

public class CustomConsumer {
	public static void main(String[] args) {
		Properties props = new Properties();
		// 定义kakfa 服务的地址，不需要将所有broker指定上 
//		props.put("bootstrap.servers", "linux01:9092");
//		props.put("bootstrap.servers", "localhost:9092");
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.7.52:9092");
		// 制定consumer group
		props.put("group.id", "g1");
		// 是否自动确认offset 
		props.put("enable.auto.commit", "true");
		// 自动确认offset的时间间隔 
		props.put("auto.commit.interval.ms", "1000");
		// key的序列化类
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		// value的序列化类 
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
		props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");

		// 消费使用 ptreader 账户
		props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='ptreader' password='pt30@123';");

		// 定义consumer 
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		
		// 消费者订阅的topic, 可同时订阅多个 
		consumer.subscribe(Arrays.asList("news"));

		while (true) {
			// 读取数据，读取超时时间为100ms 
			ConsumerRecords<String, String> records = consumer.poll(100);
			
			for (ConsumerRecord<String, String> record : records)
				System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
		}
	}
}
