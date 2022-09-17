package flink.examples.datastream._03._3_14;

import java.util.Arrays;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.util.Collector;

public class WordCountExamples {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                ParameterTool.fromArgs(args).getConfiguration());

        DataStream<String> source = env.addSource(new UserDefinedSource())
                .setParallelism(2); // 设置 StreamSource 算子的并行度

        DataStream<Tuple2<String, Integer>> singleWordDataStream = source
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Arrays.stream(line.split(" "))
                                .forEach(singleWord -> out.collect(Tuple2.of(singleWord, 1)));
                    }
                })
                .setParallelism(2); // 设置 StreamFlatMap 算子的并行度

        DataStream<Tuple2<String, Integer>> wordCountDataStream = singleWordDataStream
                .keyBy(v -> v.f0)
                .sum(1)
                .setParallelism(2); // 设置 SumAggregator 算子的并行度

        DataStreamSink<Tuple2<String, Integer>> sink = wordCountDataStream
                .print()
                .setParallelism(1); // 设置 StreamSink 算子的并行度
        env.execute();
    }

    private static class UserDefinedSource implements ParallelSourceFunction<String> {
        private volatile boolean isCancel = false;
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int i = 0;
            while (!this.isCancel) {
                i++;
                ctx.collect("I am a flinker " + i);
                Thread.sleep(1000);
            }
        }
        @Override
        public void cancel() {
            this.isCancel = true;
        }
    }
}