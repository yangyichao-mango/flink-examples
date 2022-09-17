package flink.examples.datastream._05._05_07;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class WatermarkLNotSetExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                ParameterTool.fromArgs(args).getConfiguration());

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

        OutputTag<InputModel> lateDataOutputTag = new OutputTag<InputModel>("lateData", TypeInformation.of(
                new TypeHint<InputModel>() {
                }));

        // 2.(2) 转换数据
        SingleOutputStreamOperator<InputModel> transformation = source
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .setParallelism(2)
                .filter(i -> "体育装备会场".equals(i.getProductType()))
                .keyBy(i -> i.getProductId())
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .allowedLateness(Time.minutes(2L))
                .sideOutputLateData(lateDataOutputTag)
                .reduce(new ReduceFunction<InputModel>() {
                    @Override
                    public InputModel reduce(InputModel v1, InputModel v2)
                            throws Exception {
                        v1.income += v2.income;
                        return v1;
                    }
                })
                .name("1min 滚动窗口");

        DataStream<InputModel> lateDataStream = transformation.getSideOutput(lateDataOutputTag);

        DataStreamSink<InputModel> sink = transformation.print();

        DataStreamSink<InputModel> lateSink = lateDataStream.print();

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
                                .timestamp(i % 2 == 1 ? System.currentTimeMillis() : System.currentTimeMillis() - 10 * 60 * 1000L - 16070400000L - 259200000L)
                                .productType("体育装备会场")
                                .income(3)
                                .build()
                );
                Thread.sleep(10);
            }
        }

        @Override
        public void cancel() {
            this.isCancel = true;
        }
    }

}
