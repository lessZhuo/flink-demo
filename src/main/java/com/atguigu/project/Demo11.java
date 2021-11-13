package com.atguigu.project;

import com.atguigu.beans.MywaterSensor;
import com.atguigu.beans.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：less
 * @date ：Created in 2021/11/9 21:17
 * @description：
 * @modified By：
 * @version: $
 */
public class Demo11 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> dss = env.addSource(new MywaterSensor());

        KeyedStream<WaterSensor, String> keyedStream = dss.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

        SingleOutputStreamOperator<WaterSensor> vc = keyedStream.sum("vc");

        vc.print();
        env.execute();
    }
}
