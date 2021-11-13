package com.atguigu;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author ：less
 * @date ：Created in 2021/11/8 20:43
 * @description：
 * @modified By：
 * @version: $
 */
public class Demo5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties pro = new Properties();
        pro.setProperty("bootstrap.servers","hadoop102:9092");
        pro.setProperty("group.id","consumer-group");
        pro.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        pro.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        pro.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> flink = env.addSource(new FlinkKafkaConsumer<String>("flink", new SimpleStringSchema(), pro));


        SingleOutputStreamOperator<String> flatmap = flink.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] s1 = s.split(" ");
                for (String s2 : s1) {
                    collector.collect(s2);
                }
            }
        });

        flatmap.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        }).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t2, Tuple2<String, Integer> t1) throws Exception {
                return new Tuple2<>(t2.f0,t1.f1+t2.f1);
            }
        }).print();

        env.execute();

    }
}
