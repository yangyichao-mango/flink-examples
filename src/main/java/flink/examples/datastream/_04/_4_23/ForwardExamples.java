package flink.examples.datastream._04._4_23;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class ForwardExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());

        env.setParallelism(4);

        // 2.(1) 从 App1 读入数据
        DataStreamSource<InputModel> app1Source = env.addSource(new UserDefinedSource1());

        // 2.(2) 转换数据
        DataStream<InputModel> transformation = app1Source
                // 不设置 forward，满足 forward 条件的情况下，默认就是 forward
                .map(new MapFunction<InputModel, InputModel>() {
                    @Override
                    public InputModel map(InputModel inputModel) throws Exception {
                        return inputModel;
                    }
                })
                // 主动设置 forward 模式
                .forward()
                .filter(new FilterFunction<InputModel>() {
                    @Override
                    public boolean filter(InputModel outputModel) throws Exception {
                        return true;
                    }
                });

        // 2.(3) 写出数据到控制台
        DataStreamSink<InputModel> sink = transformation.print().name("print-sink");

        // 3. 触发程序执行
        env.execute();
    }

    private static class UserDefinedSource1 extends RichParallelSourceFunction<InputModel> {

        private volatile boolean isCancel = false;

        @Override
        public void run(SourceContext<InputModel> ctx) throws Exception {
            int i = 0;
            while (!this.isCancel) {
                i++;
                ctx.collect(
                        InputModel
                                .builder()
                                .userId(i)
                                .indexOfSourceSubTask(getRuntimeContext().getIndexOfThisSubtask() + 1)
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
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class InputModel {
        private long userId;
        private int indexOfSourceSubTask;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class OutputModel {
        private long count;
        private long timestamp;
        private int indexOfSourceSubTask;
    }

}
