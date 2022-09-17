package flink.examples.datastream._04._4_36;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IterableUtils;

import lombok.extern.slf4j.Slf4j;

public class WindowExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 2.(1) 从 Kafka 读入数据
        DataStream<String> source = env.addSource(new UserDefinedSource());

        // 2.(2) 转换数据
        DataStream<String> transformation = source
                .keyBy(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        return value;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<String, String, String, TimeWindow>() {

                    private ValueStateDescriptor<Long> allPvValueStateDescriptor =
                            new ValueStateDescriptor<Long>("all-pv", Long.class);

                    @Override
                    public void process(String s, Context context, Iterable<String> elements,
                            Collector<String> out) throws Exception {

                        ListState<String> l = context.windowState().getListState(new ListStateDescriptor<String>("window-contents", String.class));

                        Object o = l.get();
                        long minutePv = IterableUtils.toStream(elements).count();
                        out.collect("窗口：" + context.window() + "，分钟 PV：" + minutePv);

                        ValueState<Long> allPvValueState = context.globalState().getState(allPvValueStateDescriptor);
                        Long allPv = allPvValueState.value();
                        allPv = null == allPv ? 0L : allPv;
                        allPv += minutePv;
                        allPvValueState.update(allPv);
                        out.collect("窗口：" + context.window() + "，历史累计 PV：" + allPv);
                    }
                });

        // 2.(3) 写出数据到自定义 Sink 中
        DataStreamSink<String> sink = transformation.print();

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

    public static class Order {
        public long id; // 订单 Id
        public int num; // 订单数量
        public String name; // 订单名称
    }

    public static class UserDefinedSource implements SourceFunction<String> {

        public volatile boolean isCancel = false;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int i = 0;
            while (!this.isCancel) {
                i++;
                ctx.collect("a" + i%2);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            this.isCancel = true;
        }
    }

}
