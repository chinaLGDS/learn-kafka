package com.bfxy.kafka.api.consumer.multithread1;

import com.bfxy.kafka.api.Const;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Mt1Test {

	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.107:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "mt1-group");
		//	自动提交的方式
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		String topic = Const.TOPIC_MT1;
		
		// coreSize
		int coreSize = 5;
		ExecutorService executorService = Executors.newFixedThreadPool(coreSize);
		
		for(int i =0; i <5; i++) {
			executorService.execute(new KafkaConsumerMt1(props, topic));
		}
		
	}
}
