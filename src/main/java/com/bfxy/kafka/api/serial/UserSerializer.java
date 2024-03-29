package com.bfxy.kafka.api.serial;

import com.bfxy.kafka.api.User;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

public class UserSerializer implements Serializer<User> {

    private String encoding = "UTF8";
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, User user) {
        if(null == user) {
            return null;
        }
        byte[] idBytes, nameBytes;

        String id = user.getId();
        String name = user.getName();

            try {
                if (id != null) {
                    idBytes = id.getBytes(encoding);
                } else {
                    idBytes = new byte[0];
                }
                if(name != null) {
                    nameBytes = name.getBytes("UTF-8");
                } else {
                    nameBytes = new byte[0];
                }

                //拼接字符数组
                //分配字符空间为两个4，分别是id与name的int类型占据4个字节
                ByteBuffer buffer = ByteBuffer.allocate( 4 + 4 + + idBytes.length + nameBytes.length);
                //	4个字节 也就是一个 int类型 : putInt 盛放 idBytes的实际真实长度
                buffer.putInt(idBytes.length);
                //	put bytes[] 实际盛放的是idBytes真实的字节数组，也就是内容
                buffer.put(idBytes);

                buffer.putInt(nameBytes.length);
                buffer.put(nameBytes);

                //array()方法获取底层byte[]
                return buffer.array();

                //UnsupportedEncodingException 更适合
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }

        return new byte[0];
        }


    @Override
    public void close() {

    }
}
