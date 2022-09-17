package flink.examples.datastream._03._3_2;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.util.Collector;

public class WordCountExamples {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                ParameterTool.fromArgs(args).getConfiguration());
        env.setParallelism(8);

        env.getCheckpointConfig().setCheckpointTimeout(TimeUnit.MINUTES.toMillis(3));
        // ck 设置
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        env.enableCheckpointing(180 * 1000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3L);

        Configuration configuration = new Configuration();
        configuration.setString("state.checkpoints.num-retained", "3");

        env.configure(configuration, Thread.currentThread().getContextClassLoader());

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 2.(1) 从 自定义数据源读入数据
        DataStream<String> source = env.addSource(new UserDefinedSource());
        // 2.(2) 对数据做处理，统计单词词频
        // flatMap：入参是一行英文语句 line，然后根据空格符分隔为一个一个的单词 singleWord
        // 出参是 Tuple2<String, Integer>，其中 f0 是单词，f1 固定为 1 代表这个单词出现了 1 次
        DataStream<Tuple2<String, Integer>> singleWordDataStream = source
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Arrays.stream(line.split(" "))
                                .forEach(singleWord -> out.collect(Tuple2.of(singleWord, 1)));
                    }
                });
        // keyBy：入参 f0 代表根据单词进行分组，将相同的单词放在一起才能统计出单词个数
        // sum：入参 代表将这个单词个数相加
        DataStream<Tuple2<String, Integer>> wordCountDataStream = singleWordDataStream
                .keyBy(v -> v.f0)
                .sum(1);
        // 2.(3) 写出数据到控制台
        DataStreamSink<Tuple2<String, Integer>> sink = wordCountDataStream.print();
        // 3. 触发程序执行
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