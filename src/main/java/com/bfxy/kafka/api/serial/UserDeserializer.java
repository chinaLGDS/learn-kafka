package com.bfxy.kafka.api.serial;

import com.bfxy.kafka.api.User;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

public class UserDeserializer implements Deserializer<User> {

    private String encoding = "UTF8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public User deserialize(String topic, byte[] data) {
        if (data == null){
            return null;
        }
        if (data.length < 8){
            throw new SerializationException("size is wrong, must be data.length >= 8");
        }
        //包裹,会将数据data存放在buffe里
        ByteBuffer buffer = ByteBuffer.wrap(data);
        //	idBytes 字节数组的真实长度
        int idLen = buffer.getInt();
        byte[] idBytes = new byte[idLen];
        buffer.get(idBytes);

        //	nameBytes 字节数组的真实长度
        int nameLen = buffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        buffer.get(nameBytes);

        String id,name;
        try {
            id = new String(idBytes,encoding);
            name = new String(nameBytes,encoding);
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("deserializaing error!");
        }


        return new User(id,name);
    }

    @Override
    public void close() {

    }
}
