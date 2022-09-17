package flink.examples.datastream._05._05_07;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class WatermarkNotDuiqiExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                ParameterTool.fromArgs(args).getConfiguration());

        env.setParallelism(10);
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
        DataStreamSource<InputModel> sport_source = env.addSource(new SportSource());
        DataStreamSource<InputModel> makeup_source = env.addSource(new MakeupSource());
        DataStreamSource<InputModel> fashion_source = env.addSource(new FashionSource());

        // 2.(2) 转换数据
        DataStream<InputModel> transformation = sport_source
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .disableChaining()
                .map(i -> i)
                .union(makeup_source.assignTimestampsAndWatermarks(watermarkStrategy)
                                .disableChaining()
                                .map(i -> i)
                        , fashion_source.assignTimestampsAndWatermarks(watermarkStrategy)
                                .disableChaining()
                                .map(i -> i))
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(60)))
                .reduce(new ReduceFunction<InputModel>() {
                    @Override
                    public InputModel reduce(InputModel v1, InputModel v2)
                            throws Exception {
                        v1.income += v2.income;
                        return v1;
                    }
                });
        DataStreamSink<InputModel> sink = transformation.print();

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
        private String productType;
    }

    private static class SportSource implements ParallelSourceFunction<InputModel> {

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
                                .timestamp(System.currentTimeMillis() - 17269200000L + 11 * 3600 * 1000L)
                                .productType("体育装备会场")
                                .income(3)
                                .build()
                );
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            this.isCancel = true;
        }
    }

    private static class MakeupSource implements ParallelSourceFunction<InputModel> {

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
                                .timestamp(System.currentTimeMillis() - 17269200000L - 10 * 3600 * 1000L + 11 * 3600 * 1000L)
                                .productType("美妆会场")
                                .income(3)
                                .build()
                );
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            this.isCancel = true;
        }
    }

    private static class FashionSource implements ParallelSourceFunction<InputModel> {

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
                                .timestamp(System.currentTimeMillis() - 17269200000L - 5 * 3600 * 1000L + 11 * 3600 * 1000L)
                                .productType("时装会场")
                                .income(3)
                                .build()
                );
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            this.isCancel = true;
        }
    }

}
