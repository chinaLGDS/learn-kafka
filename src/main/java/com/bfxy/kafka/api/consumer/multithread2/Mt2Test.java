package com.bfxy.kafka.api.consumer.multithread2;

import com.bfxy.kafka.api.Const;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class Mt2Test {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.107:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "mt2-group");
		//	手工提交
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		String topic = Const.TOPIC_MT2;
		
		
	}
}
