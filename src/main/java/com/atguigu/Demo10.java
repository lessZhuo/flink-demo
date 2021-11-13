package com.atguigu;

import com.atguigu.beans.MywaterSensor;
import com.atguigu.beans.WaterSensor;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

/**
 * @author ：less
 * @date ：Created in 2021/11/9 14:20
 * @description：
 * @modified By：
 * @version: $
 */
public class Demo10 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> DSS = env.addSource(new MywaterSensor());

        SingleOutputStreamOperator<String> DSS2 = DSS.map(WaterSensor::toString);
        DSS2.addSink(StreamingFileSink.forRowFormat(
                new Path("./output"),
                new SimpleStringEncoder<String>("UTF-8")).withRollingPolicy(
                DefaultRollingPolicy.builder().withInactivityInterval(10).withRolloverInterval(20).withMaxPartSize(1024*1024*10).build()
        ).build());

        env.execute();
    }
}
