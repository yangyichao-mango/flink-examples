package flink.examples.datastream._05._05_02;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class WindowReduceFunctionAvgExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        WatermarkStrategy<InputModel> watermarkStrategy = WatermarkStrategy
                .<InputModel>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<InputModel>() {
                    @Override
                    public long extractTimestamp(InputModel o, long l) {
                        // 我们使用了数据中自带的时间戳作为事件时间的时间戳
                        return o.getTimestamp();
                    }
                });

        // 2.(1) 从 Kafka 读入数据
        DataStream<InputModel> source = env.addSource(new UserDefinedSource());

        // 2.(2) 转换数据
        DataStream<OutputModel> transformation = source
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .map(new MapFunction<InputModel, Tuple2<InputModel, Long>>() {
                    @Override
                    public Tuple2<InputModel, Long> map(InputModel v) throws Exception {
                        return Tuple2.of(v, 1L);
                    }
                })
                .keyBy(i -> i.f0.getProductId())
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .reduce(new ReduceFunction<Tuple2<InputModel, Long>>() {
                    @Override
                    public Tuple2<InputModel, Long> reduce(Tuple2<InputModel, Long> v1, Tuple2<InputModel, Long> v2)
                            throws Exception {
                        v1.f0.income += v2.f0.income;
                        v1.f1 += v2.f1;
                        return v1;
                    }
                })
                .map(i -> OutputModel
                        .builder()
                        .productId(i.f0.productId)
                        .timestamp(i.f0.timestamp)
                        .avgIncome(((double) i.f0.income) / i.f1)
                        .build());
        DataStreamSink<OutputModel> sink = transformation.print();

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

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class OutputModel {
        private String productId;
        private long timestamp;
        private double avgIncome;
    }

    private static class UserDefinedSource implements SourceFunction<InputModel> {

        private volatile boolean isCancel = false;

        @Override
        public void run(SourceContext<InputModel> ctx) throws Exception {
            int i = 0;
            while (!this.isCancel) {
                i++;
                ctx.collect(
                        InputModel
                                .builder()
                                .productId("商品" + i % 3)
                                .timestamp(System.currentTimeMillis())
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
