package com.atguigu;

import com.atguigu.beans.MywaterSensor;
import com.atguigu.beans.WaterSensor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @author ：less
 * @date ：Created in 2021/11/9 14:33
 * @description：
 * @modified By：
 * @version: $
 */
public class KafkaProducerDemo {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStreamSource<WaterSensor> dss = env.addSource(new MywaterSensor());

        SingleOutputStreamOperator<String> DSS2 = dss.map(WaterSensor::toString);

        DSS2.addSink(new FlinkKafkaProducer<String>("hadoop102:9092","sencor",new SimpleStringSchema()));

        env.execute();
    }
}
