package flink.examples.datastream._05._05_02;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class WindowAggregateFunctionAvgProcessWindowFunctionExamples {

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

        DataStream<InputModel> source = env.addSource(new UserDefinedSource());
        AggregateFunction<InputModel, Tuple2<InputModel, Long>, OutputModel> myAggFunction = new AggregateFunction<InputModel, Tuple2<InputModel, Long>, OutputModel>() {
            @Override
            public Tuple2<InputModel, Long> createAccumulator() {
                System.out.println("AggregateFunction 初始化累加器");
                return Tuple2.of(null, 0L);
            }
            @Override
            public Tuple2<InputModel, Long> add(InputModel v, Tuple2<InputModel, Long> acc) {
                if (null == acc.f0) {
                    acc.f0 = v;
                    acc.f1 = 1L;
                } else {
                    // 累加销售额
                    acc.f0.income += v.income;
                    // 累加销量
                    acc.f1 += 1L;
                }
                return acc;
            }
            @Override
            public OutputModel getResult(Tuple2<InputModel, Long> acc) {
                System.out.println("AggregateFunction 获取结果");
                return OutputModel
                        .builder()
                        .productId(acc.f0.productId)
                        .timestamp(acc.f0.timestamp)
                        .allIncome(acc.f0.income)
                        .allCount(acc.f1)
                        .avgIncome(((double) acc.f0.income) / acc.f1)
                        .build();
            }
            @Override
            public Tuple2<InputModel, Long> merge(Tuple2<InputModel, Long> acc1, Tuple2<InputModel, Long> acc2) {
                return null;
            }
        };
        DataStream<OutputModel> transformation = source
                .assignTimestampsAndWatermarks(watermarkStrategy)
                // 根据商品类型进行分组计算
                .keyBy(i -> i.getProductId())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(myAggFunction, new ProcessWindowFunction<OutputModel, OutputModel, String, TimeWindow>() {

                    private ValueStateDescriptor<Tuple2<Long, Long>> historyInfoValueStateDescriptor =
                            new ValueStateDescriptor<Tuple2<Long, Long>>("history-info"
                                    , TypeInformation.of(
                                    new TypeHint<Tuple2<Long, Long>>() {
                                    }));

                    @Override
                    public void process(String s, Context context, Iterable<OutputModel> elements,
                            Collector<OutputModel> out) throws Exception {
                        ValueState<Tuple2<Long, Long>> historyInfoValueState =
                                context.globalState().getState(historyInfoValueStateDescriptor);
                        System.out.println("ProcessWindowFunction 获取窗口开始时间戳");
                        long windowStart = context.window().getStart();
                        for (OutputModel e : elements) {
                            System.out.println("ProcessWindowFunction 输出1min内的增量数据");
                            e.timestamp = windowStart;
                            out.collect(e);

                            Tuple2<Long, Long> historyInfoValue = historyInfoValueState.value();
                            if (null == historyInfoValue) {
                                historyInfoValue = Tuple2.of(e.allCount, e.allIncome);
                            } else {
                                historyInfoValue =
                                        Tuple2.of(e.allCount + historyInfoValue.f0, e.allIncome + historyInfoValue.f1);
                            }
                            historyInfoValueState.update(historyInfoValue);

                            System.out.println("ProcessWindowFunction 输出历史累计数据");
                            out.collect(OutputModel
                                    .builder()
                                    .productId(e.productId)
                                    .timestamp(windowStart)
                                    .allIncome(historyInfoValue.f1)
                                    .allCount(historyInfoValue.f0)
                                    .avgIncome(((double) historyInfoValue.f1) / historyInfoValue.f0)
                                    .build());
                        }
                    }
                });
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
        private long allIncome;
        private long allCount;
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
                                .productId("商品1")
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
