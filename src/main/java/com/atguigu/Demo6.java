package com.atguigu;

import com.atguigu.beans.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author ：less
 * @date ：Created in 2021/11/8 20:56
 * @description：
 * @modified By：
 * @version: $
 */
public class Demo6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.addSource(new SourceFunction<WaterSensor>() {
            boolean fla = true;

            @Override
            public void run(SourceContext<WaterSensor> ctx) throws Exception {
                Random random = new Random();
                while (fla) {
                    ctx.collect(new WaterSensor("I_" + random.nextInt(100), random.nextLong(), random.nextInt(20)));
                }
            }

            @Override
            public void cancel() {
                fla = false;
            }
        });
        waterSensorDataStreamSource.print();
        env.execute();
    }
}
