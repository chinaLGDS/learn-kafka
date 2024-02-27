package com.bfxy.kafka.api.producer;

import com.alibaba.fastjson.JSON;
import com.bfxy.kafka.api.Const;
import com.bfxy.kafka.api.User;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NormalProducer {


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.56.107:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,"normal-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // kafka消息的重试机制 该参数默认为0
        properties.put(ProducerConfig.RETRIES_CONFIG,3);


        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        User user = new User("001","国忠");
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<String, String>(Const.TOPIC_NORMAL, JSON.toJSONString(user));
        /**
         * ProducerRecord
         * topic = topic_normal
         * partition = null
         * headers = RecordHeaders(header = [], isReadOnly = false),
         * key = null,
         * value ={"id":"001","name":"中国"},
         * timestamp = null
         */
        System.err.println("新创建的消息"+producerRecord) ;
        // 一个参数的send方法本质上也是异步的，返回的是一个future对象，
//        producer.send(producerRecord);
        //同步的实现方式：
       /* Future<RecordMetadata> future = producer.send(producerRecord);
        RecordMetadata recordMetadata = future.get();
        System.err.println(String.format("分区： %s ,偏移量 : %s, 时间戳: %s",
                recordMetadata.partition(),
                recordMetadata.offset(),
                recordMetadata.timestamp()));
                */
       //带有两个参数的send方法 是完全异步化的。在回调Callback方法中得到发送消息的结果
        producer.send(producerRecord , new Callback(){
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                if ( null != exception){
                    exception.printStackTrace();
                    return;
                }
                System.err.println(String.format("分区： %s, 偏移量： %s, 时间戳: %s",
                        recordMetadata.partition(),
                        recordMetadata.offset(),
                        recordMetadata.timestamp()));
            }
        });
        producer.close();
    }

}
