package com.atguigu;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author ：less
 * @date ：Created in 2021/11/9 11:43
 * @description：
 * @modified By：
 * @version: $
 */
public class Demo8 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> DS1 = env.fromElements(1, 2, 3, 4);
        DataStreamSource<String> DS2 = env.fromElements("a", "b", "c");
        DataStreamSource<String> DS3 = env.fromElements("d", "e", "f");

        ConnectedStreams<Integer, String> conDS = DS1.connect(DS2);

        conDS.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return value.toString();
            }

            @Override
            public String map2(String value) throws Exception {
                return value;
            }
        }).print();

        DataStream<String> union = DS2.union(DS3);
        union.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s;
            }
        }).print();

        env.execute();
    }
}
