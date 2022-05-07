package flink.examples.datastream._04._3_12;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import lombok.Builder;
import lombok.Data;

public class KeybyExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());

        // 2.(1) 从 App1 读入数据
        DataStream<InputModel> app1Source = env.addSource(new UserDefinedSource1());

        // 2.(2) 转换数据
        KeyedStream<InputModel, String> transformation = app1Source
                .keyBy(new KeySelector<InputModel, String>() {
                    @Override
                    public String getKey(InputModel inputModel) throws Exception {
                        return inputModel.getProductId();
                    }
                });

        // 2.(3) 写出数据到控制台
        DataStreamSink<InputModel> sink = transformation.print();

        // 3. 触发程序执行
        env.execute();
    }

    private static class UserDefinedSource1 implements SourceFunction<InputModel> {

        private volatile boolean isCancel = false;

        @Override
        public void run(SourceContext<InputModel> ctx) throws Exception {
            int i = 0;
            while (!this.isCancel) {
                i++;
                ctx.collect(
                        InputModel
                                .builder()
                                .productId("产品" + i % 5)
                                .income(i)
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
        private String productId;
        private long income;
    }

}
