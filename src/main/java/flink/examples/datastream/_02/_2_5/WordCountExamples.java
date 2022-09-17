package flink.examples.datastream._02._2_5;

import java.util.Arrays;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountExamples {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2.(1) 从 Socket 读入数据，第一个参数是 Socket 主机名称，第二个参数是端口号
        DataStream<String> source = env.socketTextStream("localhost", 7000);
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
}