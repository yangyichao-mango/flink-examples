//package flink.examples.datastream._04._3_10;
//
//import java.util.List;
//
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSink;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//
//import com.google.common.collect.Lists;
//
//import lombok.Builder;
//import lombok.Data;
//
//public class SplitExamples {
//
//    public static void main(String[] args) throws Exception {
//        // 1. 获取执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 2.(1) 从 Kafka 读入数据
//        DataStream<InputModel1> app1Source = env.addSource(new UserDefinedSource1());
//        // 2.(1) 从 Kafka 读入数据
//        DataStream<InputModel2> app2Source = env.addSource(new UserDefinedSource2());
//
//        // 2.(3) 写出数据到控制台
//        DataStreamSink<OutputModel> sink = transformation.print();
//
//        // 3. 触发程序执行
//        env.execute();
//    }
//
//    private static class UserDefinedSource1 implements SourceFunction<InputModel1> {
//
//        private volatile boolean isCancel = false;
//
//        @Override
//        public void run(SourceContext<InputModel1> ctx) throws Exception {
//            int i = 0;
//            while (!this.isCancel) {
//                i++;
//                ctx.collect(
//                        InputModel1
//                                .builder()
//                                .username("张三")
//                                .log(Tuple2.of("页面1", "按钮1"))
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
//
//    private static class UserDefinedSource2 implements SourceFunction<InputModel2> {
//
//        private volatile boolean isCancel = false;
//
//        @Override
//        public void run(SourceContext<InputModel2> ctx) throws Exception {
//            int i = 0;
//            while (!this.isCancel) {
//                i++;
//                ctx.collect(
//                        InputModel2
//                                .builder()
//                                .username("李四")
//                                .batchLog(
//                                        Lists.newArrayList(Tuple2.of("页面1", "按钮1"), Tuple2.of("页面1", "按钮2"))
//                                )
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
//
//    @Data
//    @Builder
//    private static class InputModel1 {
//        private String username;
//        private Tuple2<String, String> log;
//    }
//
//    @Data
//    @Builder
//    private static class InputModel2 {
//        private String username;
//        private List<Tuple2<String, String>> batchLog;
//    }
//
//    @Data
//    @Builder
//    private static class OutputModel {
//        private String username;
//        private Tuple2<String, String> log;
//    }
//
//}
