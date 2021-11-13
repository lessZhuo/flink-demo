package com.atguigu.beans;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author ：less
 * @date ：Created in 2021/11/9 19:29
 * @description：
 * @modified By：
 * @version: $
 */
public class MyMarketingUserBehavior implements SourceFunction<MarketingUserBehavior> {
    boolean flag =true;
    @Override
    public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
        Random random = new Random();
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "tencent", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");
        while (flag){
            ctx.collect(new MarketingUserBehavior(
                    (long)random.nextInt(5000),
                    behaviors.get(random.nextInt(behaviors.size())),
                    channels.get(random.nextInt(channels.size())),
                    System.currentTimeMillis()
            ));

            Thread.sleep(2000);
        }
    }

    @Override
    public void cancel() {
        flag =false;
    }
}
