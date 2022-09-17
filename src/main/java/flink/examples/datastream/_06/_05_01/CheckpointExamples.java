package flink.examples.datastream._06._05_01;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class CheckpointExamples {


    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                ParameterTool.fromArgs(args).getConfiguration());

        env.setParallelism(1);

        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///Users/flink/checkpoints"));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 2.(1) 从 Kafka 读入数据
        DataStream<InputModel> source = env.addSource(new UserDefinedSource());

        // 2.(2) 转换数据
        DataStream<InputModel> transformation = source
                .keyBy(i -> i.getProductId())
                .map(new RichMapFunction<InputModel, InputModel>() {

                    private ValueStateDescriptor<Long> cumulateIncomeStateDescriptor
                            = new ValueStateDescriptor<Long>("cumulate income", Long.class);
                    private transient ValueState<Long> cumulateIncomeState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 从用户自定义函数的上下文中获取到状态实例
                        this.cumulateIncomeState = this.getRuntimeContext().getState(cumulateIncomeStateDescriptor);
                    }

                    @Override
                    public InputModel map(InputModel value) throws Exception {
                        // 通过ValueState提供的value()方法访问状态中保存的历史累计结果
                        Long cumulateIncome = this.cumulateIncomeState.value();
                        if (null == cumulateIncome) {
                            // 如果是该产品的第一条数据，则从状态中访问到的数据为 null
                            cumulateIncome = value.getIncome();
                        } else {
                            cumulateIncome += value.income;
                        }
                        // 通过ValueState提供的update方法将当前这种商品的新的累计结果更新到状态中
                        this.cumulateIncomeState.update(cumulateIncome);
                        value.setIncome(cumulateIncome);
                        return value;
                    }
                });

        // 2.(3) 写出数据到自定义 Sink 中
        DataStreamSink<InputModel> sink = transformation.print();

        // 3. 触发程序执行
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class InputModel {
        private String productId;
        private long income;
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
                                .income(i)
                                .productId("商品1")
                                .timestamp(System.currentTimeMillis())
                                .build()
                );
                Thread.sleep(1000);
                if (i == 15) {
                    throw new RuntimeException();
                }
            }
        }

        @Override
        public void cancel() {
            this.isCancel = true;
        }
    }
}