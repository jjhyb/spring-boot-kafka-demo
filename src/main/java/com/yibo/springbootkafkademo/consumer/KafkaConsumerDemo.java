package com.yibo.springbootkafkademo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author: wb-hyb441488
 * @Date: 2019/1/11 17:26
 * @Description:
 */
public class KafkaConsumerDemo {

    public static void main(String[] args) {
        //自动确认Offset
        automaticOffset();

        //手动确认Offset
//        manualOffset();
    }

    /**
     * 自动确认Offset
     */
    public static void automaticOffset(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        //group.id --> 由于在kafka中，同一组中的consumer不会读取到同一个消息，依靠groud.id设置组名
        props.put("group.id", "testGroup");
        //enable.auto.commit:true --> 设置自动提交offset
        props.put("enable.auto.commit", "true");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer kafkaConsumer = new KafkaConsumer(props);
        kafkaConsumer.subscribe(Arrays.asList("yibo"));
        while(true){
            //读取数据，读取超时时间为100ms
            ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofSeconds(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("offset = ["+record.offset()+"], key = ["+record.key()+"], value = ["+record.value()+"]");
            }
        }
    }

    /**
     * 手动确认Offset
     */
    public static void manualOffset(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        //group.id --> 由于在kafka中，同一组中的consumer不会读取到同一个消息，依靠groud.id设置组名
        props.put("group.id", "testGroup");
        //enable.auto.commit:true --> 设置手动提交offset
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer kafkaConsumer = new KafkaConsumer(props);
        kafkaConsumer.subscribe(Arrays.asList("yibo"));
        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while(true){
            //读取数据，读取超时时间为100ms

            ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofSeconds(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("offset = ["+record.offset()+"], key = ["+record.key()+"], value = ["+record.value()+"]");
            }

            /**
             * 数据达到批量要求，就写入DB，同步确认offset
             */
            if (buffer.size() >= minBatchSize) {
                //insertIntoDb(buffer);//这里为插入数据库代码
                kafkaConsumer.commitSync();
                buffer.clear();
            }
        }
    }
}
