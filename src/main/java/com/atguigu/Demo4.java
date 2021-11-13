package com.atguigu;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author ：less
 * @date ：Created in 2021/11/8 20:27
 * @description：
 * @modified By：
 * @version: $
 */
public class Demo4 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> textFile = executionEnvironment.readTextFile("hdfs://hadoop102:9820/user/flink/test.txt");
        FlatMapOperator<String, String> stringStringFlatMapOperator = textFile.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] s1 = s.split(" ");
                for (String s2 : s1) {
                    collector.collect(s2);
                }
            }
        });
        
        stringStringFlatMapOperator.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        }).groupBy(0).sum(1).print();

    }
}
