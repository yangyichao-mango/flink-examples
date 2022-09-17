//package flink.examples.datastream._04._4_36;
//
//import org.apache.flink.api.common.typeinfo.TypeHint;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.common.typeutils.TypeSerializer;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//public class SerdeExamples {
//
//    public static void main(String[] args) throws Exception {
//        // 1. 获取执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        env.setParallelism(1);
//
////        DataStream<Tuple3<Integer, Long, Order>> source = env.addSource(new UserDefinedSource());
////
////        DataStream<String> transformation = source.map(v -> v.f2.name);
////
////        DataStreamSink<String> sink = transformation.print();
//
//        // 3. 触发程序执行
////        env.execute();
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        TypeInformation<Tuple3<Integer, Long, Order>> typeInformation = TypeInformation.of(
//                new TypeHint<Tuple3<Integer, Long, Order>>() {
//                });
//
//        TypeSerializer<Tuple3<Integer, Long, Order>> typeSerializer = typeInformation.createSerializer(env.getConfig());
//
//        System.out.println(1);
//    }
//
//    public static class Order {
//        public long id; // 订单 Id
//        public int num; // 订单数量
//        public String name; // 订单名称
//    }
//
//}
