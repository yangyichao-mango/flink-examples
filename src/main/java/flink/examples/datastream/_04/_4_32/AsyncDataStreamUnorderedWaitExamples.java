//package flink.examples.datastream._04._4_32;
//
//import java.util.Collections;
//import java.util.concurrent.Callable;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.Future;
//import java.util.concurrent.TimeUnit;
//import java.util.function.Supplier;
//
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.datastream.AsyncDataStream;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSink;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.async.ResultFuture;
//import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
//import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
//
//import lombok.AllArgsConstructor;
//import lombok.Builder;
//import lombok.Data;
//import lombok.NoArgsConstructor;
//
//public class AsyncDataStreamUnorderedWaitExamples {
//
//    public static void main(String[] args) throws Exception {
//        // 1. 获取执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());
//
//        env.setParallelism(2);
//
//        // 2.(1) 从 App1 读入数据
//        DataStreamSource<InputModel> app1Source = env.addSource(new UserDefinedSource1());
//
//        // 2.(2) 转换数据
//        DataStream<Tuple2<InputModel, String>> transformation = AsyncDataStream.unorderedWait(
//                app1Source, new AsyncDatabaseRequest(), 1000, TimeUnit.MILLISECONDS, 100);
//
//        // 2.(3) 写出数据到控制台
//        DataStreamSink<Tuple2<InputModel, String>> sink = transformation.print().name("print-sink");
//
//        // 3. 触发程序执行
//        env.execute();
//    }
//
//    private static class AsyncDatabaseRequest extends RichAsyncFunction<InputModel, Tuple2<InputModel, String>> {
//        private transient ExecutorService threadPool;
//        /** 只有同步 I/O 能力的数据库客户端 */
//        private transient DatabaseClient client;
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            super.open(parameters);
//            this.threadPool = Executors.newCachedThreadPool();
//            this.client = new DatabaseClient(host, post);
//        }
//
//        @Override
//        public void asyncInvoke(InputModel input, ResultFuture<Tuple2<InputModel, String>> resultFuture) throws Exception {
//            final Future<String> result = this.threadPool.submit(new Callable<String>() {
//                @Override
//                public String call() throws Exception {
//                    return client.query(input.getCounter());
//                }
//            });
//
//            // 使用 CompletableFuture 设置 future 完成请求后要执行的回调函数
//            CompletableFuture.supplyAsync(new Supplier<String>() {
//                @Override
//                public String get() {
//                    try {
//                        return result.get();
//                    } catch (InterruptedException | ExecutionException e) {
//                        // 如果发生异常，返回 null
//                        return null;
//                    }
//                }
//            }).thenAccept((String dbResult) -> {
//                // future 完成请求后讲结果传递给 resultFuture
//                resultFuture.complete(Collections.singleton(new Tuple2<>(input, dbResult)));
//            });
//        }
//    }
//
//    private static class UserDefinedSource1 extends RichParallelSourceFunction<InputModel> {
//
//        private volatile boolean isCancel = false;
//
//        @Override
//        public void run(SourceContext<InputModel> ctx) throws Exception {
//            int i = 0;
//            while (!this.isCancel) {
//                ctx.collect(
//                        InputModel
//                                .builder()
//                                .counter(i++)
//                                .indexOfSourceSubTask(getRuntimeContext().getIndexOfThisSubtask() + 1)
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
//    @NoArgsConstructor
//    @AllArgsConstructor
//    @Builder
//    public static class InputModel {
//        private int counter;
//        private int indexOfSourceSubTask;
//    }
//
//    @Data
//    @NoArgsConstructor
//    @AllArgsConstructor
//    @Builder
//    public static class OutputModel {
//        private long count;
//        private long timestamp;
//        private int indexOfSourceSubTask;
//    }
//
//    private static class DatabaseClient {
//
//    }
//
//}
