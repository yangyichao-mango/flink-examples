package flink.examples.datastream._04._4_15;

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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class TumbleWindowExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
        // 2.(1) 从 App1 读入数据
        DataStream<InputModel> app1Source = env.addSource(new UserDefinedSource1());

        // 2.(2) 转换数据
        DataStream<InputModel> transformation = app1Source
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<InputModel>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<InputModel>() {
                                    @Override
                                    public long extractTimestamp(InputModel o, long l) {
                                        return o.getTimestamp();
                                    }
                                })
                )
                .keyBy(new KeySelector<InputModel, String>() {
                    @Override
                    public String getKey(InputModel inputModel) throws Exception {
                        return inputModel.getProductId();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new WindowFunction<InputModel, InputModel, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<InputModel> input,
                            Collector<InputModel> out) throws Exception {
                        long income = 0L;
                        String productId = null;
                        for (InputModel inputModel : input) {
                            productId = inputModel.getProductId();
                            income += inputModel.getIncome();
                        }
                        out.collect(
                                InputModel
                                .builder()
                                .productId(productId)
                                .income(income)
                                .timestamp(window.getStart())
                                .build()
                        );
                    }
                });

        // 2.(3) 写出数据到控制台
        DataStreamSink<InputModel> sink = transformation.print();

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
                                .productId("产品" + i % 5)
                                .income(i)
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
        private String productId;
        private long income;
        private long timestamp;
    }

}
