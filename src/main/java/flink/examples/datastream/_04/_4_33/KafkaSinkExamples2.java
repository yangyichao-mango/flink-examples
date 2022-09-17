package flink.examples.datastream._04._4_33;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class KafkaSinkExamples2 {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-kafka-source-example-consumer");

        SourceFunction<String> sourceFunction = new FlinkKafkaConsumer<String>(
                "flink-kafka-source-example"
                , new SimpleStringSchema()
                , properties);

        // 2.(1) 从 Kafka 读入数据
        DataStream<String> source = env.addSource(sourceFunction);

        // 2.(2) 转换数据
        DataStream<String> transformation = source.map(v -> v.split(" ")[0] + "-kafka-examples");

        SinkFunction<String> sinkFunction = new FlinkKafkaProducer<String>(
                "flink-kafka-sink-example"
                , new SimpleStringSchema()
                , properties);

        // 2.(3) 写出数据到控制台
        DataStreamSink<String> sink = transformation.addSink(sinkFunction);

        // 3. 触发程序执行
        env.execute();
    }

}
