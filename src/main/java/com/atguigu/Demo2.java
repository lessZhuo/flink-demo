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
 * @date ：Created in 2021/11/6 11:23
 * @description：
 * @modified By：
 * @version: $
 */
public class Demo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = streamExecutionEnvironment.socketTextStream("hadoop102", 9997);

        stringDataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        }).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        }).sum(1).print();

        streamExecutionEnvironment.execute();
    }
}
