package flink.examples.datastream._04._4_34;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import lombok.extern.slf4j.Slf4j;

public class UserDefinedSinkExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 2.(1) 从 Kafka 读入数据
        DataStream<String> source = env.addSource(new UserDefinedSource());

        // 2.(2) 转换数据
        DataStream<String> transformation = source.map(v -> v.split(" ")[0] + "-user-defined-sink-examples");

        // 2.(3) 写出数据到自定义 Sink 中
        DataStreamSink<String> sink = transformation.addSink(new UserDefinedSink());

        // 3. 触发程序执行
        env.execute();
    }

    @Slf4j
    private static class UserDefinedSink implements SinkFunction<String> {
        @Override
        public void invoke(String value, Context context) throws Exception {
            log.info("向外部数据汇引擎写出数据：{}", value);
        }
    }


    private static class UserDefinedSource implements ParallelSourceFunction<String> {

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
