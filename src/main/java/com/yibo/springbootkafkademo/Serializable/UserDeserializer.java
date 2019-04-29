package com.yibo.springbootkafkademo.Serializable;

import com.yibo.springbootkafkademo.entity.User;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;

/**
 * @author: wb-hyb441488
 * @Date: 2019/1/11 20:24
 * @Description:
 *
 * User反序列化器
 */
public class UserDeserializer implements Deserializer<User> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public User deserialize(String topic, byte[] bytes) {
        User user = null;
        ByteArrayInputStream inputStream = null;
        ObjectInputStream objectInputStream = null;
        try {
            inputStream = new ByteArrayInputStream(bytes);
            objectInputStream = new ObjectInputStream(inputStream);
            user = (User)objectInputStream.readObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }finally {
            if(inputStream != null){
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(objectInputStream != null){
                try {
                    objectInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return user;
    }

    @Override
    public void close() {

    }
}
