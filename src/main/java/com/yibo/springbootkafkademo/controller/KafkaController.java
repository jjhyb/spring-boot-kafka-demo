package com.yibo.springbootkafkademo.controller;

import com.yibo.springbootkafkademo.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

/**
 * @author: wb-hyb441488
 * @Date: 2019/1/11 19:30
 * @Description:
 */

@RestController
public class KafkaController {

    @Autowired
    private KafkaTemplate<String,Object> kafkaTemplate;

    @GetMapping("/message/send")
    public boolean send(@RequestParam String message){
        kafkaTemplate.send("yibo",message);
        return true;
    }

    @PostMapping("/user/save")
    public boolean saveUser(@RequestBody User user){
        kafkaTemplate.send("user",user);
        return true;
    }
}
