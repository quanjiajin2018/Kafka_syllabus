package stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class App {

    public static void main(String[] args) {

        String fromTopic = "test2";
        String toTopic = "test3";

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "LogProcessor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");

        //实例化StreamConfig
        StreamsConfig config = new StreamsConfig(props);
        //构建拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("SOURCE", fromTopic);
        builder.addProcessor("PROCESSOR", new ProcessorSupplier<byte[], byte[]>() {
            public Processor<byte[], byte[]> get() {
                return new LogProcessor();
            }
        }, "SOURCE");
        builder.addSink("SINK", toTopic, "PROCESSOR");

        //根据 StreamConfig 对象 以及用于构建拓扑的Builder 对象实例化 kafka stream
        KafkaStreams streams = new KafkaStreams(builder,props);
        streams.start();

    }

}
