package com.atguigu;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ：less
 * @date ：Created in 2021/11/6 11:41
 * @description：
 * @modified By：
 * @version: $
 */
public class Demo3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> DSS = env.socketTextStream("hadoop102", 9999);

        DSS.flatMap((FlatMapFunction<String, String>) (s, collector) -> {
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(word);
            }
        })
                .map((MapFunction<String, Tuple2<String, Integer>>) s -> new Tuple2<>(s,1))
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) stringIntegerTuple2 -> stringIntegerTuple2.f0)
                .sum(1)
                .print();

        env.execute();
    }
}
