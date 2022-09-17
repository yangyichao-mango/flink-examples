package flink.examples.datastream._04._4_36;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import flink.examples.datastream._04._3_4.protobuf.KafkaSourceModel;
import lombok.extern.slf4j.Slf4j;

public class ProtobufExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        TypeInformation<Tuple3<Integer, Long, Order>> o = TypeInformation.of(new TypeHint<Tuple3<Integer, Long, Order>>() {
        });

        Object o1 = o.createSerializer(env.getConfig());

        // 2.(1) 从 Kafka 读入数据
        DataStream<String> source = env.addSource(new UserDefinedSource());

        // 2.(2) 转换数据
        DataStream<KafkaSourceModel> transformation = source.map(v -> KafkaSourceModel.newBuilder().setName(v).build());

        // 2.(3) 写出数据到自定义 Sink 中
        DataStreamSink<KafkaSourceModel> sink = transformation.print();

        // 3. 触发程序执行
        env.execute();
    }

    @Slf4j
    private static class UserDefinedSink extends RichSinkFunction<String> {

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            log.info("向外部数据汇引擎写出数据：{}", value);
        }
    }

    public static class Order {
        public long id; // 订单 Id
        public int num; // 订单数量
        public String name; // 订单名称
    }

    public static class UserDefinedSource implements SourceFunction<String> {

        public volatile boolean isCancel = false;

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
