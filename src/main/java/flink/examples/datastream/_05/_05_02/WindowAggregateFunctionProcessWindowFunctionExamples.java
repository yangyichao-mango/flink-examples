package flink.examples.datastream._05._05_02;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowAggregateFunctionProcessWindowFunctionExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 2.(1) 从 Kafka 读入数据
        DataStream<Tuple2<String, Long>> source = env.addSource(new UserDefinedSource());

        // 2.(2) 转换数据
        DataStream<Tuple5<String, Long, Long, Double, Long>> transformation = source
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        new AggregateFunction<Tuple2<String, Long>, Tuple3<String, Long, Long>, Tuple4<String, Long,
                                Long, Double>>() {
                            @Override
                            public Tuple3<String, Long, Long> createAccumulator() {
                                System.out.println("AggregateFunction 初始化累加器");
                                return Tuple3.of(null, 0L, 0L);
                            }

                            @Override
                            public Tuple3<String, Long, Long> add(Tuple2<String, Long> v,
                                    Tuple3<String, Long, Long> acc) {
                                if (null == acc.f0) {
                                    acc.f0 = v.f0;
                                }
                                acc.f1 += v.f1;
                                acc.f2 += 1;
                                return acc;
                            }

                            @Override
                            public Tuple4<String, Long, Long, Double> getResult(Tuple3<String, Long, Long> acc) {
                                System.out.println("AggregateFunction 获取结果");
                                return Tuple4.of(acc.f0, acc.f1, acc.f2, ((double) acc.f1) / acc.f2);
                            }

                            @Override
                            public Tuple3<String, Long, Long> merge(Tuple3<String, Long, Long> acc1,
                                    Tuple3<String, Long, Long> acc2) {
                                return null;
                            }
                        },
                        new ProcessWindowFunction<Tuple4<String, Long, Long, Double>
                                , Tuple5<String, Long, Long, Double, Long>, String, TimeWindow>() {

                            private ValueStateDescriptor<Tuple2<Long, Long>> historyInfoValueStateDescriptor =
                                    new ValueStateDescriptor<Tuple2<Long, Long>>("history-info"
                                            , TypeInformation.of(
                                            new TypeHint<Tuple2<Long, Long>>() {
                                            }));

                            @Override
                            public void process(String s, Context context,
                                    Iterable<Tuple4<String, Long, Long, Double>> elements,
                                    Collector<Tuple5<String, Long, Long, Double, Long>> out) throws Exception {
                                ValueState<Tuple2<Long, Long>> historyInfoValueState =
                                        context.globalState().getState(historyInfoValueStateDescriptor);
                                System.out.println("ProcessWindowFunction 获取窗口开始时间戳");
                                long windowStart = context.window().getStart();
                                for (Tuple4<String, Long, Long, Double> e : elements) {
                                    System.out.println("ProcessWindowFunction 输出1min内的增量数据");
                                    out.collect(Tuple5.of(e.f0, e.f1, e.f2, e.f3, windowStart));

                                    Tuple2<Long, Long> historyInfoValue = historyInfoValueState.value();
                                    if (null == historyInfoValue) {
                                        historyInfoValue = Tuple2.of(e.f1, e.f2);
                                    } else {
                                        historyInfoValue =
                                                Tuple2.of(e.f1 + historyInfoValue.f0, e.f2 + historyInfoValue.f1);
                                    }
                                    historyInfoValueState.update(historyInfoValue);

                                    System.out.println("ProcessWindowFunction 输出历史累计数据");
                                    out.collect(Tuple5.of(e.f0, historyInfoValue.f0, historyInfoValue.f1,
                                            ((double) historyInfoValue.f0) / historyInfoValue.f1, windowStart));
                                }
                            }
                        });
        DataStreamSink<Tuple5<String, Long, Long, Double, Long>> sink = transformation.print();

        // 3. 触发程序执行
        env.execute();
    }

    public static class UserDefinedSource implements SourceFunction<Tuple2<String, Long>> {

        public volatile boolean isCancel = false;

        @Override
        public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
            long i = 0;
            while (!this.isCancel) {
                i++;
                ctx.collect(Tuple2.of("商品1", i));
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            this.isCancel = true;
        }
    }

}
