package cn.edu.neu.demo.ch3.customerSerializer;

import cn.edu.neu.demo.ch3.customerSerializer.Customer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 自定义序列化器
 * */
public class CustomerSerializer implements Serializer<Customer> {
    public void configure(Map<String, ?> configs, boolean isKey) {
        // 不需要配置任何
    }

    /**
     * Customer对象的序列化函数，组成如下
     * 前4字节: customerId
     * 中间4字节: customerName字节数组长度
     * 后面n字节: customerName字节数组
     * */
    public byte[] serialize(String topic, Customer data) {
        try{
            byte[] serializedName;
            int stringSize;
            if(data == null){
                return null;
            }else{
                if(data.getCustomerName() != null){
                    serializedName = data.getCustomerName().getBytes("utf-8");
                    stringSize = serializedName.length;
                } else{
                    serializedName = new byte[0];
                    stringSize = 0;
                }
            }
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + stringSize);
            buffer.putInt(data.getCustomerId());
            buffer.putInt(stringSize);
            buffer.put(serializedName);
            return buffer.array();
        } catch(Exception e){
            throw new SerializationException("Error when serializing Customer to byte[] " + e);
        }
    }

    public byte[] serialize(String topic, Headers headers, Customer data) {
        return serialize(topic, data);
    }

    public void close() {
        // 不需要关闭任何
    }
}
