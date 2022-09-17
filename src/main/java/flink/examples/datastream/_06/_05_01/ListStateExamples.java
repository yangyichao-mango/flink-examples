package flink.examples.datastream._06._05_01;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.StateTtlConfig.TtlTimeCharacteristic;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

public class ListStateExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        WatermarkStrategy<InputModel> watermarkStrategy = WatermarkStrategy
                .<InputModel>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<InputModel>() {
                    @Override
                    public long extractTimestamp(InputModel o, long l) {
                        // 我们使用了数据中自带的时间戳作为事件时间的时间戳
                        return o.getTimestamp();
                    }
                });

        // 2.(1) 从 Kafka 读入数据
        DataStream<InputModel> source = env.addSource(new UserDefinedSource());

        // 2.(2) 转换数据
        DataStream<InputModel> transformation = source
                .keyBy(i -> i.getUserId())
                .flatMap(new RichFlatMapFunction<InputModel, InputModel>() {

                    private ListStateDescriptor<String> userActionListStateDescriptor
                            = new ListStateDescriptor<String>("user action", String.class);
                    private transient ListState<String> userActionListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        StateTtlConfig stateTtlConfig = StateTtlConfig
                                // 1.ttl 时长
                                .newBuilder(Time.minutes(1))

                                // 4.过期的时间语义
                                .setTtlTimeCharacteristic(TtlTimeCharacteristic.ProcessingTime)
                                .useProcessingTime()

                                // 2.更新类型
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                // 创建和写入更新
                                .updateTtlOnCreateAndWrite()
                                // 读取和写入更新
                                .updateTtlOnReadAndWrite()

                                // 3.过期状态的访问可见性
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                // 如果还没有被删除就返回
                                .returnExpiredIfNotCleanedUp()
                                // 过期的永远不返回
                                .neverReturnExpired()

                                // 5.清除策略
                                // 做 CK 时把所有状态删除掉
                                .cleanupFullSnapshot()
                                // 增量删除，只有有状态记录访问时，才会做删除；并且他会加大任务处理延迟。
                                // 增量删除仅仅支持 HeapStateBeckend，Rocksdb 不支持！！！
                                // 每访问 1 此 state，遍历 1000 条进行删除
                                .cleanupIncrementally(1000, true)
                                // Rocksdb 状态后端在 rocksdb 做 compaction 时清除过期状态。
                                // 做 compaction 时每隔 3 个 entry，重新更新一下时间戳（用于判断是否过期）
                                .cleanupInRocksdbCompactFilter(3)
                                // 禁用 cleanup
                                .disableCleanupInBackground()
                                .build();

                        this.userActionListStateDescriptor.enableTimeToLive(stateTtlConfig);

                        this.userActionListState = this.getRuntimeContext().getListState(userActionListStateDescriptor);
                    }

                    @Override
                    public void flatMap(InputModel value, Collector<InputModel> out) throws Exception {
                        this.userActionListState.add(value.getAction());
                        Iterable<String> userActionList = (Iterable<String>) this.userActionListState.get();
//                        if (userActionList.size() == 4) {
//                            userActionList.remove(0);
//                            this.userActionListState.update(userActionList);
//                        }
                        value.setAction(userActionList.toString());
                        out.collect(value);
                    }

                });

        // 2.(3) 写出数据到自定义 Sink 中
        DataStreamSink<InputModel> sink = transformation.print();

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

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class InputModel {
        private long userId;
        private String action;
        private long timestamp;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class OutputModel {
        private long userId;
        private String action;
        private long timestamp;
    }

    public static class UserDefinedSource implements SourceFunction<InputModel> {

        public volatile boolean isCancel = false;

        @Override
        public void run(SourceContext<InputModel> ctx) throws Exception {
            int i = 0;
            while (!this.isCancel) {
                i++;
                ctx.collect(
                        InputModel
                        .builder()
                        .timestamp(System.currentTimeMillis())
                        .action("log" + i)
                        .build()
                );
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            this.isCancel = true;
        }
    }

}
