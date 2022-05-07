package flink.examples.datastream._04._3_6;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import lombok.Builder;
import lombok.Data;

public class MapExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 2.(1) 从 Kafka 读入数据
        DataStream<InputModel> source = env.addSource(new UserDefinedSource());

        // 2.(2) 转换数据
        DataStream<OutputModel> transformation = source
                .map(new MapFunction<InputModel, OutputModel>() {
                    @Override
                    public OutputModel map(InputModel inputModel) throws Exception {
                        // 0-12、12-18、18-30、30-50、50+
                        int age = inputModel.getAge();
                        String ageRange = "UNKNOWN";
                        if (age >= 0 && age < 12) {
                            ageRange = "0-12";
                        } else if (age >= 12 && age < 18) {
                            ageRange = "12-18";
                        } else if (age >= 18 && age < 30) {
                            ageRange = "18-30";
                        } else if (age >= 30 && age < 50) {
                            ageRange = "30-50";
                        } else if (age >= 50) {
                            ageRange = "50+";
                        }
                        return OutputModel
                                .builder()
                                .username(inputModel.getUsername())
                                .ageRange(ageRange)
                                .build();
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
        private String ageRange;
    }

}