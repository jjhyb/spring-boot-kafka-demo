package com.yibo.springbootkafkademo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author: wb-hyb441488
 * @Date: 2019/1/11 16:58
 * @Description:
 */

/**
 * Kafka Producer 原生 Api
 */
public class KafkaProducerDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        //kafka server的地址，集群配多个，中间，逗号隔开
        properties.put("bootstrap.servers","localhost:9092");
        //写入kafka时，leader负责一个该partion读写，
        // 当写入partition时，需要将记录同步到repli节点，all是全部同步节点都返回成功，leader才返回ack
        properties.put("acks", "all");
        //写入失败时，重试次数。当leader节点失效，一个repli节点会替代成为leader节点，此时可能出现写入失败，
        // 当retris为0时，produce不会重复。retirs重发，此时repli节点完全成为leader节点，不会产生消息丢失。
        properties.put("retries", 0);
        //produce积累到一定数据，一次发送
        properties.put("batch.size", 16384);
        //当设置了缓冲区，消息就不会即时发送，如果消息总不够条数、或者消息不够buffer大小就不发送了吗？
        // 当消息超过linger时间，也会发送
        properties.put("linger.ms", 1);
        //produce积累数据一次发送，缓存大小达到buffer.memory就发送数据
        properties.put("buffer.memory", 33554432);
        //key和value的序列化类
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer kafkaProducer = new KafkaProducer(properties);

        ProducerRecord producerRecord = new ProducerRecord("yibo",0,"message","value");
        Future<RecordMetadata> future = kafkaProducer.send(producerRecord);
        RecordMetadata recordMetadata = future.get();
        System.out.println(recordMetadata);
    }
}
