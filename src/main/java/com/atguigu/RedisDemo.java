package com.atguigu;

import com.atguigu.beans.MywaterSensor;
import com.atguigu.beans.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author ：less
 * @date ：Created in 2021/11/9 14:45
 * @description：
 * @modified By：
 * @version: $
 */
public class RedisDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> wsDSS = env.addSource(new MywaterSensor());

        FlinkJedisPoolConfig jedis = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379)
                .setDatabase(10).build();

        wsDSS.addSink(new RedisSink<WaterSensor>(
                jedis, new RedisMapper<WaterSensor>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "ws");
            }

            @Override
            public String getKeyFromData(WaterSensor waterSensor) {
                return waterSensor.getId();
            }

            @Override
            public String getValueFromData(WaterSensor waterSensor) {
                return waterSensor.getVc()+"";
            }
        }
        ));

        env.execute();

    }
}
