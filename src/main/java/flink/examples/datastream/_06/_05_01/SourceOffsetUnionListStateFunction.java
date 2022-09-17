package flink.examples.datastream._06._05_01;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class SourceOffsetUnionListStateFunction extends RichParallelSourceFunction<String>
        implements CheckpointedFunction {
    // 用于保存当前 SubTask 消费的所有的 Partition 的名称
    private transient volatile Set<String> subTaskConsumeKafkaTopicPartitions;

    // 用于保存当前 SubTask 消费的所有的 Partition 当前的偏移量
    private Map<String, Long> kafkaTopicPartitionsOffset = new HashMap<>();

    // 用于保存当前 SubTask 消费的所有的 Partition 当前的偏移量的状态
    private transient ListState<Tuple2<String, Long>> kafkaTopicPartitionsOffsetState;

    private volatile boolean isRunning = true;

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("SubTask" + (this.getRuntimeContext().getIndexOfThisSubtask() + 1) + " 执行 open 方法");
        this.initSubTaskConsumeKafkaTopicPartitions();
    }

    public void initSubTaskConsumeKafkaTopicPartitions() {
        if (null == this.subTaskConsumeKafkaTopicPartitions) {
            // 通过算子的并行度以及当前 SubTask 的下标决定当前 SubTask 消费的是哪些 Kafka Topic Partition
            int numberOfParallelSubtasks = this.getRuntimeContext().getNumberOfParallelSubtasks();
            int indexOfThisSubtask = this.getRuntimeContext().getIndexOfThisSubtask();
            // 省略初始化逻辑
//            this.subTaskConsumeKafkaTopicPartitions = ...;
            this.subTaskConsumeKafkaTopicPartitions = new HashSet<>();
            System.out.println("SubTask" + (this.getRuntimeContext().getIndexOfThisSubtask() + 1) + " 消费的 Partition 为：" + this.subTaskConsumeKafkaTopicPartitions);
            // 将当前 SubTask 消费的所有的 Partition 的偏移量置为 0
            for (String subTaskConsumeKafkaTopicPartition : subTaskConsumeKafkaTopicPartitions) {
                this.kafkaTopicPartitionsOffset.put(subTaskConsumeKafkaTopicPartition, 0L);
            }
        }
    }

    @Override
    public void run(SourceContext<String> ctx) {
        final Object lock = ctx.getCheckpointLock();

        while (isRunning) {
            // 通过 lock 保证要么在做快照，要么在输出数据
            synchronized (lock) {
                System.out.println("SubTask" + (this.getRuntimeContext().getIndexOfThisSubtask() + 1) + " 执行 run 方法");
                for (Map.Entry<String, Long> entry : kafkaTopicPartitionsOffset.entrySet()) {
                    // 输出每一个 Partition 的数据，并将偏移量分别加 1
                    ctx.collect("KafkaTopicPartition：" + entry.getKey() + "，Offset：" + entry.getValue());
                    kafkaTopicPartitionsOffset.put(entry.getKey(), entry.getValue() + 1);

                    System.out.println("当前 Partition 和偏移量分别为：" + entry.getKey() + "，" + entry.getValue());
                }
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("SubTask" + (this.getRuntimeContext().getIndexOfThisSubtask() + 1) + " 执行 initializeState 方法");

        // 通过获取 UnionListState 来获取到所有 Partition 的偏移量
        kafkaTopicPartitionsOffsetState = context.getOperatorStateStore().getUnionListState(new ListStateDescriptor<>(
                "kafka topic partitions offset",
                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() { })));

        if (context.isRestored()) {

            // 如果是 restore 的，则初始化当前 SubTask 消费的 Partition 信息
            this.initSubTaskConsumeKafkaTopicPartitions();

            for (Tuple2<String, Long> kafkaTopicPartitionOffset : kafkaTopicPartitionsOffsetState.get()) {
                for (String subTaskConsumeKafkaTopicPartition : subTaskConsumeKafkaTopicPartitions) {
                    // 只从 UnionListState 中获取到当前 SubTask 消费的 Partition 的 Offset，并保存到 kafkaTopicPartitionsOffset 中
                    if (subTaskConsumeKafkaTopicPartition.equals(kafkaTopicPartitionOffset.f0)) {
                        this.kafkaTopicPartitionsOffset.put(
                                kafkaTopicPartitionOffset.f0
                                , kafkaTopicPartitionOffset.f1);
                    }
                }
            }

            System.out.println("SubTask" + (this.getRuntimeContext().getIndexOfThisSubtask() + 1) + " 从 UnionListState 恢复得到的 Partition 偏移量为：" + this.kafkaTopicPartitionsOffset);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("SubTask" + (this.getRuntimeContext().getIndexOfThisSubtask() + 1) + " 执行 snapshotState 方法");
        kafkaTopicPartitionsOffsetState.clear();
        // 将 SubTask 消费的每一个 Partition 的偏移量保存到状态中
        System.out.println("SubTask" + (this.getRuntimeContext().getIndexOfThisSubtask() + 1) + " 消费的所有 Partition 的偏移量为：" + this.kafkaTopicPartitionsOffset);
        for (Map.Entry<String, Long> entry : this.kafkaTopicPartitionsOffset.entrySet()) {
            this.kafkaTopicPartitionsOffsetState.add(Tuple2.of(entry.getKey(), entry.getValue()));
        }
    }
}