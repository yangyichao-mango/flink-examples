package flink.examples.datastream._04._3_1;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EnvExample {

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Configuration configuration = Configuration.fromMap(parameterTool.toMap());

        // 方法 1
        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
        // 方法 2
        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.createLocalEnvironment();
        // 方法 3
        StreamExecutionEnvironment env3 = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(configuration);
        // 方法 4
        StreamExecutionEnvironment env4 = StreamExecutionEnvironment.createRemoteEnvironment(
                "localhost", 8000, "path/to/xxx.jar"
        );


    }

}
