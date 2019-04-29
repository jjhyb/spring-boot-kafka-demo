package com.yibo.springbootkafkademo.consumer;

import com.yibo.springbootkafkademo.entity.User;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @author: wb-hyb441488
 * @Date: 2019/1/11 19:36
 * @Description: 消费者
 */

@Component
public class ConsumerListener {

    @KafkaListener(topics = "yibo")
    public void onMessage(String message){
        //insertIntoDb(buffer);//这里为插入数据库代码
        System.out.println(message);
    }

    @KafkaListener(topics = "user")
    public void onMessage(User user){
        //insertIntoDb(buffer);//这里为插入数据库代码
        System.out.println(user);
    }
}
