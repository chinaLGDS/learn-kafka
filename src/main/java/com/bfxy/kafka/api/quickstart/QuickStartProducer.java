package com.bfxy.kafka.api.quickstart;

import com.alibaba.fastjson.JSON;
import com.bfxy.kafka.api.Const;
import com.bfxy.kafka.api.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class QuickStartProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();

        //1.配置生产者启动的关键属性参数
        //1.1 BOOTSTRAP_SERVERS_CONFIG,连接kafka集群服务列表，如果有多个，使用","进行分隔
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.56.107:9092");
        //1.2 CLIENT_ID_CONFIG: 这个属性的目的是标记kafka cilent的ID
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,"quickstart-producer");
        //1.3对kafka的key和value做序列化(kafka只能识别二进制数据)
        //org.apache.kafka.common.serialization.Serialization

        //key;是kafka用于做消息投递计算具体投递到对应的主题的哪一个partition而需要的
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //value:实际发送消息的内容
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //2.创建kafka生产者对象，传递properties属性参数集合
        KafkaProducer<String ,String> producer = new KafkaProducer<>(properties);

        //3.构造消息内容
        User user = new User("003","张飞");
        //需要将user对象转化为string,arg1:topic  arg2:实际消息体的内容
        ProducerRecord<String,String> record =
                new ProducerRecord<String,String>(Const.TOPIC_QUICKSTART,
                        JSON.toJSONString(user));

        //4.发送消息
        producer.send(record);

        //5.关闭生产者,生产环境中一般不关闭
        producer.close();

    }
}
