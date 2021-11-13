package com.atguigu.project;

import com.atguigu.beans.MarketingUserBehavior;
import com.atguigu.beans.MyMarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：less
 * @date ：Created in 2021/11/9 19:28
 * @description：
 * @modified By：
 * @version: $
 */
public class Flink_AC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<MarketingUserBehavior> dss = env.addSource(new MyMarketingUserBehavior());

        dss.map(new MapFunction<MarketingUserBehavior, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(MarketingUserBehavior value) throws Exception {
                return new Tuple2<>(value.getChannel()+":"+value.getBehavior(),1);
            }
        }).keyBy(0).sum(1).print();

        env.execute();
    }
}
