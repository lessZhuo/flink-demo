package com.atguigu;

import com.atguigu.beans.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author ：less
 * @date ：Created in 2021/11/9 9:37
 * @description：
 * @modified By：
 * @version: $
 */
public class Demo7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sencor1",10000L,50));
        waterSensors.add(new WaterSensor("sencor1",10001L,20));
        waterSensors.add(new WaterSensor("sencor1",10002L,40));
        waterSensors.add(new WaterSensor("sencor3",10003L,70));
        waterSensors.add(new WaterSensor("sencor2",10004L,40));
        waterSensors.add(new WaterSensor("sencor2",10005L,90));

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = executionEnvironment.fromCollection(waterSensors);

        waterSensorDataStreamSource.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor waterSensor) throws Exception {

                return waterSensor.getId();
            }
        }).process(new KeyedProcessFunction<String, WaterSensor, String>() {
            Map<String,Integer> map=new HashMap<String,Integer>();
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                Integer orDefault = map.getOrDefault(value.getId(), 0);
                map.put(value.getId(),orDefault+value.getVc());
                out.collect(value.getId()+":"+map.get(value.getId()));
            }
        }).print();

        executionEnvironment.execute();

    }
}
