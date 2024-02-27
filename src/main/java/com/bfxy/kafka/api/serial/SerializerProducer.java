package com.bfxy.kafka.api.serial;

import com.alibaba.fastjson.JSON;
import com.bfxy.kafka.api.Const;
import com.bfxy.kafka.api.User;
import com.bfxy.kafka.api.interceptor.CustomProducerInterceptor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SerializerProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.107:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "serial-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //添加序列化value
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserSerializer.class.getName());
        //properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //自定义序列化之后，可以直接使用User对象
        KafkaProducer<String, User> producer = new KafkaProducer<>(properties);
        for(int i = 0; i <2; i ++) {
            User user = new User("00" + i, "张三");
            ProducerRecord<String, User> record =
                    new ProducerRecord<String, User>(Const.TOPIC_SERIAL,
                            user);
            producer.send(record);
        }
        producer.close();

    }
}
