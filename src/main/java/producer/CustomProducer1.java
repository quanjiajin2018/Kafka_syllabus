package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class CustomProducer1 {
    public static void main(String[] args) {

        //1.0 配置生产者属性
        Properties props = new Properties();
        //1.1 配置kafka集群节点地址，可以配置多个
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop103:9092");
        //1.2 配置发送的消息是否等待应答
        props.put("acks", "all");
        //1.3 配置消息发送失败的重试
        props.put("retries", "0");
        //1.4 批量处理数据的大小：16kb
        props.put("batch.size", "16384");
        //1.5 设置批量处理数据的延迟，单位：ms
        props.put("linger.ms", "5");
        //1.6 设置内在缓冲区的大小
        props.put("buffer.memoery", "33445532");
        //1.7 数据在发送之前，一定要序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //1.8 value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //1.9 设置分区
        props.put("partitioner.class" ,"partitioner.CustomPartitioner");

        //2.实例化KafkaProducer
        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(props);

        for(int i = 0 ;i< 50;i++){
            //3.调用Producer 的send 方法，进行消息的发送,每条待发送的消息，都必需封闭为一个Record 对象
            producer.send( new ProducerRecord<String, String>("test2","hello" + i));
        }

        //4.close 以释放资源
        producer.close();

    }
}
