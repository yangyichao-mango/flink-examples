//package flink.examples.datastream._05._05_06;
//
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSink;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
//import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
//import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
//import org.apache.flink.util.Collector;
//
//import lombok.AllArgsConstructor;
//import lombok.Builder;
//import lombok.Data;
//import lombok.NoArgsConstructor;
//
//public class CountWindowExamples {
//
//    public static void main(String[] args) throws Exception {
//        // 1. 获取执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
//                ParameterTool.fromArgs(args).getConfiguration());
//
//        env.setParallelism(1);
//
//        // 2.(1) 从 Kafka 读入数据
//        DataStream<ShowInputModel> showSource = env.addSource(new ShowUserDefinedSource());
//
//        // 2.(2) 转换数据
//        DataStream<OutputModel> transformation = showSource.keyBy(show -> show.getUserId() + show.getProductId() + show.getUniqueId())
//                .countWindow(10)
//                .apply(new WindowFunction<ShowInputModel, Object, String, GlobalWindow>() {
//                })
//                .process(new ProcessWindowFunction<ShowInputModel, OutputModel, String, GlobalWindow>() {
//                    @Override
//                    public void process(String s, Context context, Iterable<ShowInputModel> elements,
//                            Collector<OutputModel> out) throws Exception {
//                        System.out.println(1);
//                    }
//                });
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
//    public static class ShowInputModel {
//        private long userId;
//        private String productId;
//        private long uniqueId;
//        private long timestamp;
//    }
//
//    @Data
//    @NoArgsConstructor
//    @AllArgsConstructor
//    @Builder
//    public static class OutputModel {
//        private long userId;
//        private String productId;
//        private long uniqueId;
//        private long timestamp;
//        private long clickTimestamp;
//    }
//
//    @Data
//    @NoArgsConstructor
//    @AllArgsConstructor
//    @Builder
//    public static class ClickInputModel {
//        private long userId;
//        private String productId;
//        private long uniqueId;
//        private long timestamp;
//    }
//
//    private static class ShowUserDefinedSource implements SourceFunction<ShowInputModel> {
//        private volatile boolean isCancel = false;
//        @Override
//        public void run(SourceContext<ShowInputModel> ctx) throws Exception {
//            int i = 0;
//            while (!this.isCancel) {
//                i++;
//                long timestamp = System.currentTimeMillis();
//                ctx.collect(
//                        ShowInputModel
//                                .builder()
//                                .userId(1)
//                                .productId("商品1")
//                                .timestamp(timestamp)
//                                .build()
//                );
//                Thread.sleep(1000);
//            }
//        }
//        @Override
//        public void cancel() {
//            this.isCancel = true;
//        }
//    }
//
//
//    private static class ClickUserDefinedSource implements SourceFunction<ClickInputModel> {
//        private volatile boolean isCancel = false;
//        @Override
//        public void run(SourceContext<ClickInputModel> ctx) throws Exception {
//            int i = 0;
//            while (!this.isCancel) {
//                i++;
//                long timestamp = System.currentTimeMillis();
//                ctx.collect(
//                        ClickInputModel
//                                .builder()
//                                .userId(1)
//                                .productId("商品" + i % 3)
//                                .timestamp(timestamp)
//                                .build()
//                );
//                Thread.sleep(1000);
//            }
//        }
//        @Override
//        public void cancel() {
//            this.isCancel = true;
//        }
//    }
//
//}
