package flink.examples.datastream._04._4_20;

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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.google.common.collect.Lists;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class BoundedOutOfOrdernessStrategyExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        // 2.(1) 从 App1 读入数据
        DataStream<InputModel> app1Source = env.addSource(new UserDefinedSource1());

        // 2.(2) 转换数据
        DataStream<OutputModel> transformation = app1Source
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<InputModel>forBoundedOutOfOrderness(Duration.ofSeconds(60))
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
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new WindowFunction<InputModel, OutputModel, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<InputModel> input,
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
