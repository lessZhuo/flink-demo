package com.atguigu.beans;

import com.atguigu.beans.WaterSensor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author ：less
 * @date ：Created in 2021/11/9 14:03
 * @description：
 * @modified By：
 * @version: $
 */
public class MywaterSensor implements SourceFunction<WaterSensor> {
    boolean flag = true;
    @Override
    public void run(SourceContext<WaterSensor> ctx) throws Exception {
        Random random = new Random();
        while (flag) {
            ctx.collect(new WaterSensor("I_" + random.nextInt(100), random.nextLong(), random.nextInt(20)));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
