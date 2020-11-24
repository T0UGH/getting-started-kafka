package cn.edu.neu.demo.ch3.simpleProducer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * 通过kafka客户端，创建生产者并发送消息
 * @author t0ugh
 * */
public class Main {
    public static void main(String[] args) {
        // 配置属性  47.94.139.116:9092
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "47.94.139.116:9092");
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("acks", "all");

        // 根据配置创建Kafka生产者
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(kafkaProperties);

        // 创建ProducerRecord，它是一种消息的数据结构
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
            "sun", "s1", "cn dota best dota");

        // 发送消息

        kafkaProducer.send(producerRecord, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                System.out.println(recordMetadata);
                e.printStackTrace();
            }
        });
        kafkaProducer.flush();
    }
}
