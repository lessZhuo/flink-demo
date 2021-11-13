package com.atguigu;

import com.atguigu.beans.MywaterSensor;
import com.atguigu.beans.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：less
 * @date ：Created in 2021/11/9 13:59
 * @description：
 * @modified By：
 * @version: $
 */
public class Demo9 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> dss = env.addSource(new MywaterSensor());
        dss.map(new RichMapFunction<WaterSensor, Integer>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("start");
            }

            @Override
            public void close() throws Exception {
                System.out.println("stop");
            }

            @Override
            public Integer map(WaterSensor waterSensor) throws Exception {
                return waterSensor.getVc()*waterSensor.getVc();
            }
        }).print();

        env.execute();

    }
}
