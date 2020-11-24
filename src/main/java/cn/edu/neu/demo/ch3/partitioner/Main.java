package cn.edu.neu.demo.ch3.partitioner;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * 通过kafka客户端，创建生产者并发送消息
 * 运行时请修改kafka broker地址
 * @author t0ugh
 * */
public class Main {
    public static void main(String[] args) {
        // 配置属性  47.94.139.116:9092
        Properties kafkaProperties = new Properties();
        // fixme: 运行时请修改47.94.139.116:9092为自己的kafka broker地址
        kafkaProperties.put("bootstrap.servers", "47.94.139.116:9092");
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 设置自定义分区器
        kafkaProperties.put("partitioner.class", "cn.edu.neu.demo.ch3.partitioner.BananaPartitioner");

        kafkaProperties.put("acks", "all");

        // 根据配置创建Kafka生产者
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(kafkaProperties);

        // 创建普通消息的ProducerRecord
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
            "sun", "s1", "cn dota best dota");

        // 发送普通消息
        kafkaProducer.send(producerRecord, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                System.out.println(recordMetadata);
                e.printStackTrace();
            }
        });

        // 创建Banana消息的ProducerRecord，把键设置为Banana
        ProducerRecord<String, String> bananaRecord = new ProducerRecord<>(
            "sun", "Banana", "cn banana best banana");

        // 发送普通消息
        kafkaProducer.send(bananaRecord, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                System.out.println(recordMetadata);
                e.printStackTrace();
            }
        });

        kafkaProducer.flush();
    }
}
