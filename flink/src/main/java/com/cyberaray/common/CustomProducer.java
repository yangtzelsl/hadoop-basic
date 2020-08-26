package com.cyberaray.common;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Properties;

public class CustomProducer {
	public static void main(String[] args) throws InterruptedException {
		Properties props = new Properties();
		// Kafka服务端的主机名和端口号
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.7.52:9092");
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

		props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
		props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
		// 生产使用 cdpush 账户
		props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='cdpush' password='cd@pt30.cetc';");

		KafkaProducer<String, String> producer = new KafkaProducer<>(props);
		// mock data
		for (int i = 1; i<= 10; i++ ){
			JSONObject out = new JSONObject();
			out.put("vendor", "pt");
			out.put("platform", "news");
			out.put("push_time", "1595902335");
			out.put("uuid", "c403a21331d3ee73a364f6ee9faceade"+i);
			out.put("table_type", "post");

			JSONObject inner = new JSONObject();
			inner.put("news_url", "http://news.stnn.cc/fzsjxw/2020/"+i+"/771746.shtml");
			inner.put("site_type", i);
			inner.put("gather_time", "2020-07-28 10:12:14");
			inner.put("site_name", "星岛环球网");
			inner.put("repeat_count", 0);
			inner.put("summary", "");
			inner.put("author", "星岛环球网"+i);
			inner.put("title", "杨安队任茂名市副市长");
			inner.put("keywords", "茂名市人大常委会,茂名市人大常委会教科文卫侨外事工作委员会,茂名市人大常委会财政经济,职务");
			inner.put("type", "pt_kafka_news");
			inner.put("nation", "中国香港");
			inner.put("post_id", "2145e86aede2943706b40405e9331a81");
			inner.put("file_info", "");
			inner.put("content", "7月27日，茂名市十二届人大常委会第三十六次会议审议通过任免名单");
			inner.put("comment_count", 0);
			inner.put("publish_time", "2020-07-28 08:32:00");
			inner.put("@version", "1");
			inner.put("like_count", 0);
			inner.put("plate", "聚焦大湾区");
			inner.put("collect_company", "PT");

			out.put("data", inner);

			producer.send(new ProducerRecord<String, String>("news", out.toString()));
			Thread.sleep(3000);
		}

		producer.close();
	}
}

