package com.cyberaray.common;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class CDHCustomProducer {
	public static void main(String[] args) throws InterruptedException {
		Properties props = new Properties();
		// Kafka服务端的主机名和端口号
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.7.63:9092");
		// 等待所有副本节点的应答
		props.put("acks", "all");
		// 消息发送最大尝试次数
		props.put("retries", 0);
		// 一批消息处理大小
		props.put("batch.size", 16384);
		// 请求延时
		props.put("linger.ms", 1);
		// 发送缓存区内存大小
		props.put("buffer.memory", 33554432);
		// key序列化
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// value序列化
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		//props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
		//props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
		// 生产使用 cdpush 账户
		//props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='cdpush' password='cd@pt30.cetc';");

		KafkaProducer<String, String> producer = new KafkaProducer<>(props);
		// mock data
		for (int i = 1; i<= 10; i++ ){

			JSONObject out = new JSONObject();

			out.put("vendor", "pt"+i);
			out.put("push_time", "1595902335");

			producer.send(new ProducerRecord<String, String>("news", out.toString()));
			Thread.sleep(1000);
		}

		producer.close();
	}
}

