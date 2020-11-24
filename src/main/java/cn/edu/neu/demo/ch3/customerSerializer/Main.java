package cn.edu.neu.demo.ch3.customerSerializer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

/**
 * 通过kafka客户端，创建生产者并发送消息
 * @author t0ugh
 * */
public class Main {
    public static void main(String[] args) {

        // 配置Producer属性  47.94.139.116:9092
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "47.94.139.116:9092");
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "cn.edu.neu.demo.ch3.customerSerializer.CustomerSerializer");
        kafkaProperties.put("acks", "all");

        // 根据配置创建Kafka生产者
        KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<String, Customer>(kafkaProperties);

        Customer customer = new Customer(1, "Tim");

        // 创建ProducerRecord，它是一种消息的数据结构
        ProducerRecord<String, Customer> producerRecord = new ProducerRecord<String, Customer>(
            "customer", "s1", customer);

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
