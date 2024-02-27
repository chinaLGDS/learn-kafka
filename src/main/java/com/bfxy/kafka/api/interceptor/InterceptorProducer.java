package com.bfxy.kafka.api.interceptor;

import com.alibaba.fastjson.JSON;
import com.bfxy.kafka.api.Const;
import com.bfxy.kafka.api.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class InterceptorProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.107:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "interceptor-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //	添加生产者拦截器属性, 生产者拦截器可以配置多个
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, CustomProducerInterceptor.class.getName());
//        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, CustomProducerInterceptor.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for(int i = 0; i <10; i ++) {
            User user = new User("00" + i, "张三");
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(Const.TOPIC_INTERCEPTOR,
                            JSON.toJSONString(user));
            producer.send(record);
        }
        producer.close();
    }
}
