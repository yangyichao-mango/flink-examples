package flink.examples.datastream._04._3_9;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import lombok.Builder;
import lombok.Data;

public class UnionExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.(1) 从 Kafka 读入数据
        DataStream<InputModel> app1Source = env.addSource(new UserDefinedSource());
        // 2.(1) 从 Kafka 读入数据
        DataStream<InputModel> app2Source = env.addSource(new UserDefinedSource());
        // 2.(1) 从 Kafka 读入数据
        DataStream<InputModel> app3Source = env.addSource(new UserDefinedSource());

        // 2.(2) 转换数据
        DataStream<InputModel> transformation = app1Source.union(app2Source, app3Source);

        // 2.(3) 写出数据到控制台
        DataStreamSink<InputModel> sink = transformation.print();

        // 3. 触发程序执行
        env.execute();
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
                                .username("张三")
                                .age((i % 65))
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
    @Builder
    private static class InputModel {
        private String username;
        private int age;
    }

    @Data
    @Builder
    private static class OutputModel {
        private String username;
        private Tuple2<String, String> log;
    }

}
