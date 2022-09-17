package flink.examples.datastream._05._05_01;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IterableUtils;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

public class ProcessWindowFunctionGlobalStateExamples {

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
        DataStream<String> transformation = source
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(i -> i.getProductId())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<InputModel, String, String, TimeWindow>() {

                    // 使用 ValueState 来存储历史累计PV
                    private ValueStateDescriptor<Long> allPvValueStateDescriptor =
                            new ValueStateDescriptor<Long>("all-pv", Long.class);

                    @Override
                    public void process(String productId, Context context, Iterable<InputModel> elements,
                            Collector<String> out) throws Exception {
                        // 集合的大小就是这一分钟的访问PV数
                        long minutePv = IterableUtils.toStream(elements).count();
                        // 将这1min内的访问PV输出
                        out.collect("商品：" + productId + "，窗口：" + context.window() + "，分钟 PV：" + minutePv);

                        // 从globalState中获取到历史累计的PV数，我们可以暂且理解状态就是Flink任务本地的一个变量
                        ValueState<Long> allPvValueState = context.globalState().getState(allPvValueStateDescriptor);
                        Long allPv = allPvValueState.value();
                        // 第一次访问时，值为null，则默认赋值为0
                        allPv = null == allPv ? 0L : allPv;
                        // 新的历史累计PV = 旧的历史累计 + 当前这一分钟的累计值
                        allPv += minutePv;
                        // 将新的历史累计PV存储进状态中
                        allPvValueState.update(allPv);
                        // 将历史累计的访问PV输出
                        out.collect("商品：" + productId + "窗口：" + context.window() + "，历史累计 PV：" + allPv);
                    }
                });

        // 2.(3) 写出数据到自定义 Sink 中
        DataStreamSink<String> sink = transformation.print();

        // 3. 触发程序执行
        env.execute();
    }

    @Slf4j
    private static class UserDefinedSink extends RichSinkFunction<String> {

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            log.info("向外部数据汇引擎写出数据：{}", value);
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class InputModel {
        private long userId;
        private String productId;
        private long timestamp;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class OutputModel {
        private long count;
        private long timestamp;
    }

    public static class UserDefinedSource implements SourceFunction<InputModel> {

        public volatile boolean isCancel = false;

        @Override
        public void run(SourceContext<InputModel> ctx) throws Exception {
            int i = 0;
            while (!this.isCancel) {
                i++;
                ctx.collect(
                        InputModel
                        .builder()
                        .userId(1)
                        .productId("商品" + i % 2)
                        .timestamp(System.currentTimeMillis())
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
