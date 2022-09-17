//package flink.examples.datastream._06._06_04;
//
//import org.apache.flink.api.common.functions.ReduceFunction;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSink;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//
//import lombok.AllArgsConstructor;
//import lombok.Builder;
//import lombok.Data;
//import lombok.NoArgsConstructor;
//
//public class ProductIncomeExamples {
//
//    public static void main(String[] args) throws Exception {
//        // 1. 获取执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
//                ParameterTool.fromArgs(args).getConfiguration());
//
//        env.setParallelism(1);
//
//        env.getCheckpointConfig().setFailOnCheckpointingErrors();
//
//        env.getCheckpointConfig().setCheckpointStorage("hdfs:///my/checkpoint/dir");
//
//        // 2.(1) 从 Kafka 读入数据
//        DataStream<InputModel> source = env.addSource(new UserDefinedSource());
//
//        // 2.(2) 转换数据
//        SingleOutputStreamOperator<InputModel> transformation = source
//                .keyBy(i -> i.getId())
//                .reduce(new ReduceFunction<InputModel>() {
//                    @Override
//                    public InputModel reduce(InputModel value1, InputModel value2) throws Exception {
//                        value2.income += value1.income;
//                        return value2;
//                    }
//                });
//
//        DataStreamSink<InputModel> sink = transformation.print();
//        // 3. 触发程序执行
//        env.execute();
//    }
//
//    @Data
//    @NoArgsConstructor
//    @AllArgsConstructor
//    @Builder
//    public static class InputModel {
//        private String id;
//        private long income;
//        private long timestamp;
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
//                                .timestamp(System.currentTimeMillis())
//                                .id("商品" + i % 2)
//                                .income(3)
//                                .build()
//                );
//                Thread.sleep(10);
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
