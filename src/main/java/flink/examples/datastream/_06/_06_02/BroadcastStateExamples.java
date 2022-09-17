package flink.examples.datastream._06._06_02;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import com.google.common.collect.Lists;

import lombok.Builder;
import lombok.Data;

public class BroadcastStateExamples {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());

        env.setParallelism(10);

        // 定义广播状态MapState的描述符
        MapStateDescriptor<String, ConfigModel> configModelStateDescriptor = new MapStateDescriptor<>(
                "ConfigModelBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<ConfigModel>() { }));

        // 通过广播状态描述符定义出广播数据流
        BroadcastStream<ConfigModel> configSource = env.addSource(new ConfigSourceFunction())
                .setParallelism(1)
                // 设置数据传输策略为 broadcast
                .broadcast(configModelStateDescriptor);

        // 定义主数据流
        DataStream<OutputModel> transformation = env.addSource(new InputModelSourceFunction())
                .connect(configSource)
                .process(new BroadcastProcessFunction<InputModel, ConfigModel, OutputModel>() {

                    @Override
                    public void processElement(InputModel value, ReadOnlyContext ctx, Collector<OutputModel> out)
                            throws Exception {
                        ReadOnlyBroadcastState<String, ConfigModel> configModelBroadcastState =
                                ctx.getBroadcastState(configModelStateDescriptor);
                        ConfigModel configModel = configModelBroadcastState.get("latest-config");
                        if (null != configModel) {
                            for (Pattern pattern : configModel.patterns) {
                                if (pattern.matcher(value.info).matches()) {
                                    out.collect(OutputModel
                                            .builder()
                                            .pattern(pattern.toString())
                                            .id(value.id)
                                            .info(value.info)
                                            .build());
                                }
                            }
                        }
                    }

                    @Override
                    public void processBroadcastElement(ConfigModel value, Context ctx, Collector<OutputModel> out)
                            throws Exception {
                        BroadcastState<String, ConfigModel> configModelBroadcastState =
                                ctx.getBroadcastState(configModelStateDescriptor);

                        configModelBroadcastState.put("latest-config", value);
                    }
                });

        // 2.(3) 写出数据到控制台
        DataStreamSink<OutputModel> sink = transformation.print().name("print-sink");

        // 3. 触发程序执行
        env.execute();
    }

    private static class InputModelSourceFunction extends RichParallelSourceFunction<InputModel> {

        private volatile boolean isCancel = false;

        @Override
        public void run(SourceContext<InputModel> ctx) throws Exception {
            int i = 0;
            while (!this.isCancel) {
                i++;
                ctx.collect(
                        InputModel
                                .builder()
                                .id(i)
                                .info("A")
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

    private static class ConfigSourceFunction extends RichSourceFunction<ConfigModel> {

        private volatile boolean isCancel = false;

        @Override
        public void run(SourceContext<ConfigModel> ctx) throws Exception {
            while (!this.isCancel) {
                ctx.collect(
                        ConfigModel
                                .builder()
                                // 此处省略从配置中心获取到最新的 Pattern 的处理逻辑
                                .patterns(Lists.newArrayList(Pattern.compile("[ABC]")))
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

    @Data
    @Builder
    public static class InputModel {
        private long id;
        private String info;
    }

    @Data
    @Builder
    public static class ConfigModel {
        private List<Pattern> patterns;
    }

    @Data
    @Builder
    public static class OutputModel {
        private String pattern;
        private long id;
        private String info;
    }

}
