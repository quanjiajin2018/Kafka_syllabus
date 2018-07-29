package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class CustomConsumer {

    public static void main(String[] args) {

        //1.配置消费者属性
        Properties props = new Properties();
        //1.1 定义kafka的实例服务地址
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        //设置消费组
        props.put("group.id", "g1");
        //是否自动确认offset
        props.put("enable.auto.commit", "true");
        // key的序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value的序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //2.创建消费者实例
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //4.释放资源
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                if(consumer != null){
                    consumer.close();
                    System.out.println("释放资源!!! GetOut");
                }
            }
        }));

        //订阅消息主题
        consumer.subscribe(Arrays.asList("test2"));

        //3.拉消息
        while (true) {

            ConsumerRecords<String, String> con = consumer.poll(100);
            for (ConsumerRecord<String, String> record : con) {
                System.out.println(record.offset() + "————" + record.key() + "- - -" + record.value());
            }
        }

    }


}
