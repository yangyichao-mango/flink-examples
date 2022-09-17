package flink.examples.datastream._06._06_02;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import com.google.common.collect.Lists;

import lombok.Builder;
import lombok.Data;

public class BroadcastConnectExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(ParameterTool.fromArgs(args).getConfiguration());

        env.setParallelism(10);

        DataStream<ConfigModel> configSource = env.addSource(new ConfigSourceFunction())
                .setParallelism(1)
                // 设置数据传输策略为 broadcast
                .broadcast();

        DataStream<OutputModel> transformation = env.addSource(new InputModelSourceFunction())
                .connect(configSource)
                .flatMap(new RichCoFlatMapFunction<InputModel, ConfigModel, OutputModel>() {

                    private volatile ConfigModel configModel;

                    @Override
                    public void flatMap1(InputModel value, Collector<OutputModel> out) throws Exception {
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
                    public void flatMap2(ConfigModel value, Collector<OutputModel> out) throws Exception {
                        this.configModel = value;
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
