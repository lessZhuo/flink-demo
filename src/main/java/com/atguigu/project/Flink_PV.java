package com.atguigu.project;

import com.atguigu.beans.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * @author ：less
 * @date ：Created in 2021/11/9 18:56
 * @description：
 * @modified By：
 * @version: $
 */
public class Flink_PV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dss = env.readTextFile("./input/UserBehavior.csv");

//        dss.map(new MapFunction<String, UserBehavior>() {
//            @Override
//            public UserBehavior map(String value) throws Exception {
//                String[] split = value.split(",");
//                return new UserBehavior(Long.valueOf(split[0]),Long.valueOf(split[1]),
//                        Integer.valueOf(split[2]),split[3],Long.valueOf(split[4]));
//            }
//        }).filter(new FilterFunction<UserBehavior>() {
//            @Override
//            public boolean filter(UserBehavior value) throws Exception {
//                return "pv".equals(value.getBehavior());
//            }
//        }).process(new ProcessFunction<UserBehavior, Integer>() {
//            int sum =0;
//            @Override
//            public void processElement(UserBehavior value, Context ctx, Collector<Integer> out) throws Exception {
//                sum+=1;
//                out.collect(sum);
//            }
//        }).print();

        dss.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] split = value.split(",");
                return new UserBehavior(Long.valueOf(split[0]),Long.valueOf(split[1]),
                        Integer.valueOf(split[2]),split[3],Long.valueOf(split[4]));
            }
        }).process(new ProcessFunction<UserBehavior, Integer>() {
            Set set =new HashSet<Long>();
            @Override
            public void processElement(UserBehavior value, Context ctx, Collector<Integer> out) throws Exception {
                set.add(value.getUserId());
                out.collect(set.size());
            }
        }).print();

        env.execute();
    }
    }
