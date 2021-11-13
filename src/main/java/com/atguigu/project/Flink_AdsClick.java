package com.atguigu.project;

import com.atguigu.beans.AdsClickLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：less
 * @date ：Created in 2021/11/9 19:41
 * @description：
 * @modified By：
 * @version: $
 */
public class Flink_AdsClick {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dss = env.readTextFile("./input/AdClickLog.csv");
        dss.map(new MapFunction<String, AdsClickLog>() {
            @Override
            public AdsClickLog map(String value) throws Exception {
                String[] split = value.split(",");

                return new AdsClickLog(Long.valueOf(split[0]),Long.valueOf(split[1]),split[2],
                        split[3],Long.valueOf(split[4]));
            }
        }).map(new MapFunction<AdsClickLog, Tuple2<Tuple2<String,Long>,Integer>>() {
            @Override
            public Tuple2<Tuple2<String, Long>, Integer> map(AdsClickLog value) throws Exception {
                return new Tuple2<>(new Tuple2<>(value.getProvince(),value.getAdId()),1);
            }
        }).keyBy(0).sum(1).print();

        env.execute();
    }
}
