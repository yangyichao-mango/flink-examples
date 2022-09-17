package flink.examples.datastream._03._3_1;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Lists;

/**
 * 非分布式应用
 */
public class Examples {
    public static void main(String[] args) throws Exception {
        // 1. 数据源是英文语句 line
        List<String> lines = Lists.newArrayList("I am a flinker", "I am a flinker");

        // 2. 统计词频
        Map<String, Long> result = lines
                .stream()
                .flatMap(new Function<String, Stream<String>>() {
                    @Override
                    public Stream<String> apply(String line) {
                        return Arrays.stream(line.split(" "));
                    }
                })
                .collect(Collectors.groupingBy(singleWord -> singleWord, Collectors.counting()));

        // 3. 写出数据到控制台
        System.out.println(result);
    }
}