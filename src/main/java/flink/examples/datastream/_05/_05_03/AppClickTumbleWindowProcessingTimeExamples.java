//package flink.examples.datastream._05._05_02;
//
//import java.time.Duration;
//
//import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.AggregateFunction;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSink;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.streaming.api.windowing.time.Time;
//
//import lombok.AllArgsConstructor;
//import lombok.Builder;
//import lombok.Data;
//import lombok.NoArgsConstructor;
//
//public class AppClickTumbleWindowProcessingTimeExamples {
//
//    public static void main(String[] args) throws Exception {
//        // 1. 获取执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        env.setParallelism(1);
//        WatermarkStrategy<InputModel> watermarkStrategy = WatermarkStrategy
//                .<InputModel>forBoundedOutOfOrderness(Duration.ZERO)
//                .withTimestampAssigner(new SerializableTimestampAssigner<InputModel>() {
//                    @Override
//                    public long extractTimestamp(InputModel o, long l) {
//                        // 我们使用了数据中自带的时间戳作为事件时间的时间戳
//                        return o.getTimestamp();
//                    }
//                });
//
//        // 2.(1) 从 Kafka 读入数据
//        DataStream<InputModel> source = env.addSource(new UserDefinedSource());
//        AggregateFunction<InputModel, Tuple2<InputModel, Long>, OutputModel> myAggFunction = new AggregateFunction<InputModel, Tuple2<InputModel, Long>, OutputModel>() {
//            @Override
//            public Tuple2<InputModel, Long> createAccumulator() {
//                System.out.println("AggregateFunction 初始化累加器");
//                return Tuple2.of(null, 0L);
//            }
//            @Override
//            public Tuple2<InputModel, Long> add(InputModel v, Tuple2<InputModel, Long> acc) {
//                if (null == acc.f0) {
//                    acc.f0 = v;
//                    acc.f1 = 1L;
//                } else {
//                    // 累加销售额
//                    acc.f0.income += v.income;
//                    // 累加销量
//                    acc.f1 += 1L;
//                }
//                return acc;
//            }
//            @Override
//            public OutputModel getResult(Tuple2<InputModel, Long> acc) {
//                System.out.println("AggregateFunction 获取结果");
//                return OutputModel
//                        .builder()
//                        .productId(acc.f0.productId)
//                        .timestamp(acc.f0.timestamp)
//                        .allIncome(acc.f0.income)
//                        .allCount(acc.f1)
//                        .avgIncome(((double) acc.f0.income) / acc.f1)
//                        .build();
//            }
//            @Override
//            public Tuple2<InputModel, Long> merge(Tuple2<InputModel, Long> acc1, Tuple2<InputModel, Long> acc2) {
//                return null;
//            }
//        };
//        DataStream<OutputModel> transformation = source
//                .assignTimestampsAndWatermarks(WatermarkStrategy
//                        .<InputModel>forBoundedOutOfOrderness(Duration.ZERO)
//                        .withTimestampAssigner(new SerializableTimestampAssigner<InputModel>() {
//                            @Override
//                            public long extractTimestamp(InputModel o, long l) {
//                                // 使用了数据中自带的时间戳
//                                return o.getTimestamp();
//                            }
//                        }))
//                // 根据商品类型进行分组计算
//                .keyBy(i -> i.getUserId())ntTimeWindows.of(Time.seconds(10)))
//                .aggregate(myAggFunction);
//        DataStreamSink<OutputModel> sink = transformation.print();
//
//        // 3. 触发程序执行
//        env.execute();
//    }
//
//    @Data
//    @NoArgsConstructor
//    @AllArgsConstructor
//    @Builder
//    public static class InputModel {
//        private String userId;
//        private long timestamp;
//    }
//
//    @Data
//    @NoArgsConstructor
//    @AllArgsConstructor
//    @Builder
//    public static class OutputModel {
//        private String productId;
//        private long timestamp;
//        private double avgIncome;
//        private long allIncome;
//        private long allCount;
//    }
//
//    private static class UserDefinedSource implements SourceFunction<InputModel> {
//
//        private volatile boolean isCancel = false;
//
//        @Override
//        public void run(SourceContext<InputModel> ctx) throws Exception {
//            int i = 0;
//            while (!this.isCancel) {
//                i++;
//                ctx.collect(
//                        InputModel
//                                .builder()
//                                .productId("商品" + i % 3)
//                                .timestamp(System.currentTimeMillis())
//                                .income(3)
//                                .build()
//                );
//                Thread.sleep(1000);
//            }
//        }
//
//        @Override
//        public void cancel() {
//            this.isCancel = true;
//        }
//    }
//}
