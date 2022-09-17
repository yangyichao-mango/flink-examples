package flink.examples.datastream._05._05_04;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
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

public class UserDefinedStrategyExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                ParameterTool.fromArgs(args).getConfiguration());

        env.setParallelism(1);

        // 2.(1) 从 Kafka 读入数据
        DataStream<InputModel> source = env.addSource(new UserDefinedSource());
        WatermarkStrategy<InputModel> periodicWatermarkGenerator = new WatermarkStrategy<InputModel>() {
            @Override
            public WatermarkGenerator<InputModel> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<InputModel>() {
                    @Override
                    public void onEvent(InputModel event, long eventTimestamp, WatermarkOutput output) {
                        output.emitWatermark(new Watermark(eventTimestamp - (60 * 1000L)));
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        // 什么也不做
                        System.out.println(1);
                    }
                };
            }

            @Override
            public TimestampAssigner<InputModel> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                return (e, lastRecordTimestamp) -> e.getTimestamp();
            }
        };

        WatermarkStrategy<InputModel> punctuatedWatermarkGenerator = new WatermarkStrategy<InputModel>() {
            @Override
            public WatermarkGenerator<InputModel> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<InputModel>() {

                    private long currentMaxTimestamp;

                    @Override
                    public void onEvent(InputModel event, long eventTimestamp, WatermarkOutput output) {
                        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        output.emitWatermark(new Watermark(currentMaxTimestamp - (60 * 1000L)));
                    }
                };
            }

            @Override
            public TimestampAssigner<InputModel> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                return (e, lastRecordTimestamp) -> e.getTimestamp();
            }
        };

        WatermarkStrategy<InputModel> ascendingTimestampsWatermarkGenerator =
                WatermarkStrategy
                .<InputModel>forMonotonousTimestamps()
                .withTimestampAssigner((e, lastRecordTimestamp) -> e.getTimestamp());



        WatermarkStrategy<InputModel> boundedOutOfOrdernessWatermarkGenerator =
                WatermarkStrategy
                .<InputModel>forBoundedOutOfOrderness(Duration.ofSeconds(30L))
                .withTimestampAssigner((e, lastRecordTimestamp) -> e.getTimestamp());

        // 2.(2) 转换数据
        DataStream<InputModel> transformation = source
                .assignTimestampsAndWatermarks(punctuatedWatermarkGenerator)
                .keyBy(i -> i.getProductId())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
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
