package flink.examples.datastream._06._06_02;

import java.util.HashMap;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class HashMapExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 2.(1) 从 Kafka 读入数据
        DataStream<InputModel> source = env.addSource(new UserDefinedSource());

        // 2.(2) 转换数据
        DataStream<InputModel> transformation = source
                .keyBy(i -> i.getProductId())
                .map(new RichMapFunction<InputModel, InputModel>() {
                    private transient HashMap<String, Long> cumulateIncomeState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        this.cumulateIncomeState = new HashMap<>();
                    }

                    @Override
                    public InputModel map(InputModel value) throws Exception {
                        Long cumulateIncome = this.cumulateIncomeState.get(value.getProductId());
                        if (null == cumulateIncome) {
                            cumulateIncome = value.getIncome();
                        } else {
                            cumulateIncome += value.getIncome();
                        }
                        this.cumulateIncomeState.put(value.getProductId(), cumulateIncome);
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
            }
        }

        @Override
        public void cancel() {
            this.isCancel = true;
        }
    }

}
