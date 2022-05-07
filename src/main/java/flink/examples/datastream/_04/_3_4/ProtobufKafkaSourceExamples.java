package flink.examples.datastream._04._3_4;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import flink.examples.datastream._04._3_4.protobuf.KafkaSourceModel;
import flink.examples.datastream._04._3_4.schema.ProtobufDeserializationSchema;

public class ProtobufKafkaSourceExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 定义连接 Kafka Source 的一些配置信息
        KafkaSource<KafkaSourceModel> kafkaSource = KafkaSource.<KafkaSourceModel>builder()
                .setBootstrapServers("localhost:9092") // 消费的 Kafka bootstrap server
                .setTopics("flink-kafka-source-example") // 消费的 Kafka topic
                .setGroupId("flink-kafka-source-example-consumer") // consumer 的 group-id
                .setStartingOffsets(OffsetsInitializer.earliest()) // 消费 Kafka 的位点
                .setValueOnlyDeserializer(new ProtobufDeserializationSchema()) // flink 反序列化 Kafka 消息的 schema
                .build();

        // 2.(1) 从 Kafka 读入数据
        DataStream<KafkaSourceModel> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 2.(2) 转换数据
        DataStream<String> transformation = source.map(v -> v.getName() + "-kafka-examples");

        // 2.(3) 写出数据到控制台
        DataStreamSink<String> sink = transformation.print();

        // 3. 触发程序执行
        env.execute();
    }

}
