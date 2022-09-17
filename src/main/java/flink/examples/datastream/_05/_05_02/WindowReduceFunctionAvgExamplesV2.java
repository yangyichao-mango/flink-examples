package flink.examples.datastream._05._05_02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowReduceFunctionAvgExamplesV2 {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 2.(1) 从 Kafka 读入数据
        DataStream<Tuple2<String, Long>> source = env.addSource(new UserDefinedSource());

        // 2.(2) 转换数据
        DataStream<Tuple2<String, Double>> transformation = source
                .map(new MapFunction<Tuple2<String, Long>, Tuple3<String, Long, Long>>() {
                    @Override
                    public Tuple3<String, Long, Long> map(Tuple2<String, Long> v) throws Exception {
                        return Tuple3.of(v.f0, v.f1, 1L);
                    }
                })
                .keyBy(new KeySelector<Tuple3<String, Long, Long>, String>() {
                    @Override
                    public String getKey(Tuple3<String, Long, Long> v) throws Exception {
                        return v.f0;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<Tuple3<String, Long, Long>>() {
                    @Override
                    public Tuple3<String, Long, Long> reduce(Tuple3<String, Long, Long> v1,
                            Tuple3<String, Long, Long> v2)
                            throws Exception {
                        return Tuple3.of(v1.f0, v1.f1 + v2.f1, v1.f2 + v2.f2);
                    }
                })
                .map(new MapFunction<Tuple3<String, Long, Long>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Tuple3<String, Long, Long> v) throws Exception {
                        return Tuple2.of(v.f0, ((double) v.f1) / v.f2);
                    }
                });
        DataStreamSink<Tuple2<String, Double>> sink = transformation.print();

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
                ctx.collect(Tuple2.of("商品" + i % 2, i));
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            this.isCancel = true;
        }
    }

}
