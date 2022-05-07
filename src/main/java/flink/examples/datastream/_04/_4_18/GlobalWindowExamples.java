package flink.examples.datastream._04._4_18;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import com.google.common.collect.Lists;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class GlobalWindowExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        // 2.(1) 从 App1 读入数据
        DataStream<InputModel> app1Source = env.addSource(new UserDefinedSource1());

        // 2.(2) 转换数据
        DataStream<OutputModel> transformation = app1Source
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<InputModel>forBoundedOutOfOrderness(Duration.ofSeconds(60L))
                                .withTimestampAssigner(new SerializableTimestampAssigner<InputModel>() {
                                    @Override
                                    public long extractTimestamp(InputModel o, long l) {
                                        return o.getTimestamp();
                                    }
                                })
                )
                .keyBy(new KeySelector<InputModel, Long>() {
                    @Override
                    public Long getKey(InputModel inputModel) throws Exception {
                        return inputModel.getUserId();
                    }
                })
                .window(GlobalWindows.create())
                .trigger(new Trigger<InputModel, GlobalWindow>() {
                    @Override
                    public TriggerResult onElement(InputModel element, long timestamp, GlobalWindow window,
                            TriggerContext ctx) throws Exception {
                        return null;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx)
                            throws Exception {
                        return null;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx)
                            throws Exception {
                        return null;
                    }

                    @Override
                    public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {

                    }
                })
                .apply(new WindowFunction<InputModel, OutputModel, Long, GlobalWindow>() {
                    @Override
                    public void apply(Long s, GlobalWindow window, Iterable<InputModel> input,
                            Collector<OutputModel> out) throws Exception {
                        out.collect(
                                OutputModel
                                        .builder()
                                        // 商品个数就是输入数据的 size
                                        .count(Lists.newArrayList(input).size())
                                        .build()
                        );
                    }
                });

        // 2.(3) 写出数据到控制台
        DataStreamSink<OutputModel> sink = transformation.print();

        // 3. 触发程序执行
        env.execute();
    }

    private static class UserDefinedSource1 implements SourceFunction<InputModel> {

        private volatile boolean isCancel = false;

        @Override
        public void run(SourceContext<InputModel> ctx) throws Exception {
            int i = 0;
            while (!this.isCancel) {
                i++;
                ctx.collect(
                        InputModel
                                .builder()
                                .userId(i)
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

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class InputModel {
        private long userId;
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

}
