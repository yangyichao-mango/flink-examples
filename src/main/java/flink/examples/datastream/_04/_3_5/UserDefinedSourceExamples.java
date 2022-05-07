package flink.examples.datastream._04._3_5;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class UserDefinedSourceExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 2.(1) 从 Kafka 读入数据
        DataStream<String> source = env.addSource(new UserDefinedSource());

        // 2.(2) 转换数据
        DataStream<String> transformation = source.map(v -> v.split(" ")[0] + "-user-defined-source-examples");

        // 2.(3) 写出数据到控制台
        DataStreamSink<String> sink = transformation.print();

        // 3. 触发程序执行
        env.execute();
    }

    private static class UserDefinedSource implements SourceFunction<String> {

        private volatile boolean isCancel = false;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int i = 0;
            while (!this.isCancel) {
                i++;
                ctx.collect(i + "");
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            this.isCancel = true;
        }
    }

    private static class RichUserDefinedSource extends RichSourceFunction<String> {

        private volatile boolean isCancel = false;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 1. 初始化外部链接资源
            super.open(parameters);
            // 2. 获取状态
            IntCounter i = this.getRuntimeContext().getIntCounter("int-counter");
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int i = 0;
            while (!this.isCancel) {
                i++;
                ctx.collect(i + "");
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            this.isCancel = true;
        }

        @Override
        public void close() throws Exception {
            // 关闭外部链接资源
            super.close();
        }
    }

    private static class UserDefinedParallelSource
            implements ParallelSourceFunction<String> {

        private volatile boolean isCancel = false;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int i = 0;
            while (!this.isCancel) {
                i++;
                ctx.collect(i + "");
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            this.isCancel = true;
        }
    }

    private static class RichUserDefinedParallelSource extends RichParallelSourceFunction<String> {

        private volatile boolean isCancel = false;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int i = 0;
            while (!this.isCancel) {
                i++;
                ctx.collect(i + "");
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            this.isCancel = true;
        }
    }

}
