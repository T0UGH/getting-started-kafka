package cn.edu.neu.demo.ch3.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * 自定义分区器
 * 若键为Banana则放入最后一个分区，若键不为Banana则散列到其他分区
 * */
public class BananaPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        // 分区信息列表
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);

        //分区数量
        int partitionsAmount = partitions.size();

        // 如果只有一个分区，那全都放到partition0就完事了
        if(partitionsAmount == 1){
            return 0;
        }

        if(keyBytes == null || ! (key instanceof String)){
            throw new InvalidRecordException("We expect all messages to have customer name as key");
        }

        if(key.equals("Banana")){
            return partitionsAmount - 1;
        }

        return (Math.abs(Utils.murmur2(keyBytes)) % (partitionsAmount - 1));

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
