//package flink.examples.datastream._05._05_01;
//
//import org.apache.flink.api.java.tuple.Tuple;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSink;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
//import org.apache.flink.streaming.api.windowing.assigners.DynamicEventTimeSessionWindows;
//import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
//import org.apache.flink.streaming.api.windowing.evictors.Evictor;
//import org.apache.flink.streaming.api.windowing.triggers.Trigger;
//import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.streaming.api.windowing.windows.Window;
//import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
//import org.apache.flink.util.Collector;
//
//import flink.examples.datastream._04._3_4.protobuf.KafkaSourceModel;
//import lombok.extern.slf4j.Slf4j;
//
//public class KeyedStreamWindowExamples {
//
//    public static void main(String[] args) throws Exception {
//        // 1. 获取执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        env.setParallelism(1);
//
//        // 2.(1) 从 Kafka 读入数据
//        DataStream<String> source = env.addSource(new UserDefinedSource());
//
//        // 2.(2) 转换数据
//        DataStream<KafkaSourceModel> transformation = source.map(v -> KafkaSourceModel.newBuilder().setName(v).build())
//                .keyBy()
//                .window(DynamicEventTimeSessionWindows.withDynamicGap(
//                        new SessionWindowTimeGapExtractor<KafkaSourceModel>() {
//                            @Override
//                            public long extract(KafkaSourceModel element) {
//                                return 0;
//                            }
//                        }))
//                .trigger(new Trigger<KafkaSourceModel, Window>() {
//                    @Override
//                    public TriggerResult onElement(KafkaSourceModel element, long timestamp, Window window,
//                            TriggerContext ctx) throws Exception {
//                        return null;
//                    }
//
//                    @Override
//                    public TriggerResult onProcessingTime(long time, Window window, TriggerContext ctx)
//                            throws Exception {
//                        return null;
//                    }
//
//                    @Override
//                    public TriggerResult onEventTime(long time, Window window, TriggerContext ctx) throws Exception {
//                        return null;
//                    }
//
//                    @Override
//                    public void clear(Window window, TriggerContext ctx) throws Exception {
//                    }
//                })
//                .evictor(new Evictor<KafkaSourceModel, Window>() {
//                    @Override
//                    public void evictBefore(Iterable<TimestampedValue<KafkaSourceModel>> elements, int size,
//                            Window window, EvictorContext evictorContext) {
//
//                    }
//
//                    @Override
//                    public void evictAfter(Iterable<TimestampedValue<KafkaSourceModel>> elements, int size,
//                            Window window, EvictorContext evictorContext) {
//
//                    }
//                })
//                .apply(new WindowFunction<KafkaSourceModel, KafkaSourceModel, Tuple, TimeWindow>() {
//                    @Override
//                    public void apply(Tuple tuple, TimeWindow window, Iterable<KafkaSourceModel> input,
//                            Collector<KafkaSourceModel> out) throws Exception {
//
//                    }
//                })
//                ;
//
//        // 2.(3) 写出数据到自定义 Sink 中
//        DataStreamSink<KafkaSourceModel> sink = transformation.print();
//
//        // 3. 触发程序执行
//        env.execute();
//    }
//
//    @Slf4j
//    private static class UserDefinedSink extends RichSinkFunction<String> {
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            super.open(parameters);
//        }
//
//        @Override
//        public void invoke(String value, Context context) throws Exception {
//            log.info("向外部数据汇引擎写出数据：{}", value);
//        }
//    }
//
//    public static class Order {
//        public long id; // 订单 Id
//        public int num; // 订单数量
//        public String name; // 订单名称
//    }
//
//    public static class UserDefinedSource implements SourceFunction<String> {
//
//        public volatile boolean isCancel = false;
//
//        @Override
//        public void run(SourceContext<String> ctx) throws Exception {
//            int i = 0;
//            while (!this.isCancel) {
//                i++;
//                ctx.collect(i + "");
//                Thread.sleep(1000);
//            }
//        }
//
//        @Override
//        public void cancel() {
//            this.isCancel = true;
//        }
//    }
//
//}
