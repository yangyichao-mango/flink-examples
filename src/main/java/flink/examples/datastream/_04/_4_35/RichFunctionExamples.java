package flink.examples.datastream._04._4_35;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import flink.examples.datastream._04._4_36.LambdaExamples.UserDefinedSource;

public class RichFunctionExamples {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        Configuration conf = new Configuration();
        conf.setString("global-params-1", "value");
        env.getConfig().setGlobalJobParameters(conf);

        // 2.(1) 从 Kafka 读入数据
        DataStream<String> source = env.addSource(new UserDefinedSource());

        // 2.(2) 转换数据
        DataStream<String> transformation = source.map(
                new RichMapFunction<String, String>() {

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                        int maxNumberOfParallelSubtasks = getRuntimeContext().getMaxNumberOfParallelSubtasks();
                        int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
                        String globalParams1 = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap().get("global-params-1");
                        String taskName = getRuntimeContext().getTaskNameWithSubtasks();

                        System.out.println("indexOfThisSubtask:" + indexOfThisSubtask);
                        System.out.println("maxNumberOfParallelSubtasks:" + maxNumberOfParallelSubtasks);
                        System.out.println("numberOfParallelSubtasks:" + numberOfParallelSubtasks);
                        System.out.println("globalParams1:" + globalParams1);
                        System.out.println("taskName:" + taskName);
                    }

                    @Override
                    public String map(String value) throws Exception {
                        return value;
                    }
                }
        );
        // 3. 触发程序执行
        env.execute();
    }


}
