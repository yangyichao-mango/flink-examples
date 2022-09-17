package flink.examples.datastream._04._4_33;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


// https://www.jianshu.com/p/5491d16e6abd
// https://www.jianshu.com/p/dd2578d47ff6

// > cd zookeeper-3.4.10/bin //切换到 bin目录
//> ./zkServer.sh start //启动
//
//> ./kafka-server-start /usr/local/etc/kafka/server.properties &
//
//> kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic
// flink-kafka-source-example
//
//
//> kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic
// flink-kafka-sink-example
public class KafkaSinkExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 定义连接 Kafka Source 的一些配置信息
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092") // 消费的 Kafka bootstrap server
                .setTopics("flink-kafka-source-example") // 消费的 Kafka topic
                .setGroupId("flink-kafka-source-example-consumer") // consumer 的 group-id
                .setStartingOffsets(OffsetsInitializer.earliest()) // 消费 Kafka 的位点
                .setValueOnlyDeserializer(new SimpleStringSchema()) // flink 反序列化 Kafka 消息的 schema
                .build();

        // 2.(1) 从 Kafka 读入数据
        DataStream<String> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 2.(2) 转换数据
        DataStream<String> transformation = source.map(v -> v.split(" ")[0] + "-kafka-examples");

        Integer[] i = null;

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(
                    KafkaRecordSerializationSchema
                            .builder()
                            .setTopic("flink-kafka-sink-example")
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // 2.(3) 写出数据 Kafka
        DataStreamSink<String> sink = transformation.sinkTo(kafkaSink);

        // 3. 触发程序执行
        env.execute();
    }

}
