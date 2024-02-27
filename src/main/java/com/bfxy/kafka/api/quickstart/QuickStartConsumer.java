package com.bfxy.kafka.api.quickstart;


import com.bfxy.kafka.api.Const;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author nly
 */
public class QuickStartConsumer {

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        //1.配置属性参数
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.107:9092");
        //key;是kafka用于做消息投递计算具体投递到对应的主题的哪一个partition而需要的
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //value:实际发送消息的内容
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //非常重要的属性配置,与我们消费者订阅组有关系
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "topic-quickstart");
        //常规属性：会话连接超时时间
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        //常规属性：消费者提交offset：自动提交&手工提交，默认是自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,5000);

        //2.创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //3.订阅你感兴趣的主题:Const.TOPIC_QUICKSTART
        consumer.subscribe(Collections.singletonList(Const.TOPIC_QUICKSTART));

        System.err.println("quickstart consumer started ...");

        //4.采用拉取的方式消费数据
        while (true){
                 //等待多久进行一次数据的拉取
                 //拉取TOPIC_QUICKSTART主题里面所有的消息
                 //topic和partition是一对多的关系，一个topic可以有多个partition
                 ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(3000));
                 //因为消息是在partition中存储的，所以需要遍历partition集合
                 for (TopicPartition topicPartition : records.partitions()){
                     //通过topicPartition获取制定的消息集合，就是获取到当前topicPartition下面所有的消息
                     // 在records对象中的数据集合
                     List<ConsumerRecord<String,String>> partitionRecords = records.records(topicPartition);
                     //获取到每一个TopicPartition
                     String topic =topicPartition.topic();
                     //获取当前topicPartition下的消息条数
                     int size = partitionRecords.size();

                     System.out.println(String.format("--- 获取topic： %s,  分区位置 : %s, 消息总数： %s",
                             topic,
                             topicPartition.partition(),
                             size));

                     for(int i = 0; i <  size; i++){
                         ConsumerRecord<String,String> consumerRecord = partitionRecords.get(i);
                         // 实际数据内容
                         String value = consumerRecord.value();
                         // 当前获取的消息的偏移量
                         long offset = consumerRecord.offset();
                         //ISR : High Watermark,如果要提交的话，比如提交当前消息的offset
                         //表示下一次从什么位置(offset)拉取消息
                         long commitOffset = offset + 1;

                         System.err.println(String.format("获取实际消息value： %s,  消息offset: %s, 提交offset： %s",
                                 value,offset,commitOffset));
                     }
                 }
             }










    }



}
