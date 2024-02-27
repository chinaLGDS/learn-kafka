package com.bfxy.kafka.api.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public class CustomConsumerInterceptor implements ConsumerInterceptor<String,String> {


    //消费者接到消息处理之前的拦截器
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        System.err.println("------  消费者前置处理器，接收消息  ------");
        return records;
    }

    //自动提交，配置的时间内(假设为5s)打印一次
    //手动提交，当你真正完成提交动作的时候
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((tp, offset) -> {
            System.err.println("消费者处理完成，" + "分区:" + tp + ", 偏移量：" + offset);
        });
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        // TODO Auto-generated method stub
    }
}
