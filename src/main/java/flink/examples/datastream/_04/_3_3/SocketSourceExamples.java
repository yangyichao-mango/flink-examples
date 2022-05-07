package flink.examples.datastream._04._3_3;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SocketSourceExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 2.(1) 从 Socket 读入数据
        DataStream<String> source = env
                .socketTextStream("localhost", 7000);

        // 2.(2) 转换数据
        DataStream<String> transformation = source.map(v -> v.split(" ")[0] + "examples");

        // 2.(3) 写出数据到控制台
        DataStreamSink<String> sink = transformation.print();

        // 3. 触发程序执行
        env.execute();
    }

}
