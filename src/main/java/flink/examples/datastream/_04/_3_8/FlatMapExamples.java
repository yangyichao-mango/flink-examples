package flink.examples.datastream._04._3_8;

import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import com.google.common.collect.Lists;

import lombok.Builder;
import lombok.Data;

public class FlatMapExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 2.(1) 从 Kafka 读入数据
        DataStream<InputModel> source = env.addSource(new UserDefinedSource());

        // 2.(2) 转换数据
        DataStream<OutputModel> transformation = source
                .flatMap(new FlatMapFunction<InputModel, OutputModel>() {
                    @Override
                    public void flatMap(InputModel inputModel, Collector<OutputModel> collector) throws Exception {
                        for (Tuple2<String, String> log : inputModel.getBatchLog()) {
                            if (log.f0.equals("页面1")) {
                                collector.collect(
                                        OutputModel
                                                .builder()
                                                .username(inputModel.getUsername())
                                                .log(log)
                                                .build()
                                );
                            }
                        }
                    }
                });

        // 2.(3) 写出数据到控制台
        DataStreamSink<OutputModel> sink = transformation.print();

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
                        .batchLog(
                                Lists.newArrayList(Tuple2.of("a", "b"), Tuple2.of("c", "d"))
                        )
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
        private List<Tuple2<String, String>> batchLog;
    }

    @Data
    @Builder
    private static class OutputModel {
        private String username;
        private Tuple2<String, String> log;
    }

}
