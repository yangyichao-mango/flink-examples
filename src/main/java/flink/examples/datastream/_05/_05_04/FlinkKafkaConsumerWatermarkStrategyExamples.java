package flink.examples.datastream._05._05_04;

import java.util.Properties;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class FlinkKafkaConsumerWatermarkStrategyExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                ParameterTool.fromArgs(args).getConfiguration());

        env.setParallelism(1);

        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String o, long l) {
                        // 我们使用了数据中自带的时间戳作为事件时间的时间戳
                        return System.currentTimeMillis();
                    }
                });

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-kafka-source-example-consumer");

        SourceFunction<String> sourceFunction = new FlinkKafkaConsumer<String>(
                "flink-kafka-source-example"
                , new SimpleStringSchema()
                , properties)
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // 2.(1) 从 Kafka 读入数据
        DataStream<String> source = env.addSource(sourceFunction)
                .setParallelism(2);

        // 3. 触发程序执行
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class InputModel {
        private String productId;
        private long timestamp;
        private long income;
    }

    private static class UserDefinedSource implements SourceFunction<InputModel> {
        private volatile boolean isCancel = false;
        @Override
        public void run(SourceContext<InputModel> ctx) throws Exception {
            int i = 0;
            while (!this.isCancel) {
                i++;
                long timestamp = System.currentTimeMillis();
                ctx.collect(
                        InputModel
                                .builder()
                                .productId("商品" + i % 3)
                                .timestamp(timestamp)
                                .income(3)
                                .build()
                );
                Thread.sleep(1000);
            }
        }
        @Override
        public void cancel() {
            this.isCancel = true;
        }
    }

}
