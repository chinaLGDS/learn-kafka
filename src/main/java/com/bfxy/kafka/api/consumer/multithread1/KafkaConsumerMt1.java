package com.bfxy.kafka.api.consumer.multithread1;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaConsumerMt1 implements Runnable {

	private KafkaConsumer<String, String> consumer;

	private volatile boolean isRunning;

	private static AtomicInteger counter = new AtomicInteger(0);

	private String consumerName;

	public KafkaConsumerMt1(Properties properties, String topic) {
		this.consumer =new KafkaConsumer<>(properties);
		this.consumer.subscribe(Arrays.asList(topic));
		this.consumerName = "KafkaConsumerMt1-" + counter.getAndIncrement();
		System.err.println(this.consumerName + " started ");
	}

	@Override
	public void run() {
		try{
			while (isRunning){
				//包含所有topic下的消息内容(只包括传进来的topic)
				ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
				for (TopicPartition topicPartition : consumerRecords.partitions()){
					//获取topic名称
					String topic = topicPartition.topic();
					// 根据具体的topicPartition 去获取对应的topicPartitions下的数据集合
					List<ConsumerRecord<String,String>> partitionList = consumerRecords.records(topicPartition);
					//获取partitionList中数据的个数，便于遍历
					int size = partitionList.size();
					for(int i = 0; i < size; i++){

						ConsumerRecord<String,String> consumerRecord = partitionList.get(i);
						// do execute messages
						String message = consumerRecord.value();
						//如果是获取下一步需要执行的offset= messageOffset+1
						long messageOffset = consumerRecord.offset();
						System.err.println("当前消费者："+ consumerName
								+ ",消息内容：" + message
								+ ", 消息的偏移量: " + messageOffset
								+ "当前线程：" + Thread.currentThread().getName());
					}
				}
			}

		}finally {
			if (consumer != null){
				consumer.close();
			}
		}

	}

	public boolean isRunning() {
		return isRunning;
	}

	public void setRunning(boolean isRunning) {
		this.isRunning = isRunning;
	}
}
