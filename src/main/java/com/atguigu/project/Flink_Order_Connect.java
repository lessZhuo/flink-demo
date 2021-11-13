package com.atguigu.project;

import com.atguigu.beans.OrderEvent;
import com.atguigu.beans.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * @author ：less
 * @date ：Created in 2021/11/9 19:57
 * @description：
 * @modified By：
 * @version: $
 */
public class Flink_Order_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> orderDSS = env.readTextFile("./input/OrderLog.csv");
        DataStreamSource<String> receiptDSS = env.readTextFile("./input/ReceiptLog.csv");

        SingleOutputStreamOperator<OrderEvent> orderDS = orderDSS.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new OrderEvent(
                        Long.valueOf(split[0]),
                        split[1], split[2],
                        Long.valueOf(split[3])
                );
            }
        });

        SingleOutputStreamOperator<TxEvent> receiptDS = receiptDSS.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new TxEvent(
                        split[0], split[1], Long.valueOf(split[2])
                );
            }
        });

        ConnectedStreams<OrderEvent, TxEvent> connect = orderDS.connect(receiptDS);
        connect.keyBy(new KeySelector<OrderEvent, String>() {
            @Override
            public String getKey(OrderEvent value) throws Exception {
                return value.getTxId();
            }
        }, new KeySelector<TxEvent, String>() {
            @Override
            public String getKey(TxEvent value) throws Exception {
                return value.getTxId();
            }
        }).process(new CoProcessFunction<OrderEvent, TxEvent, String>() {
            Set<String> orderSet = new HashSet<>();
            Set<String> TxSet = new HashSet<>();

            @Override
            public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                if (TxSet.contains(value.getTxId())){
                    TxSet.remove(value.getTxId());
                    out.collect(value.getOrderId()+"对账成功");
                }else {
                    orderSet.add(value.getTxId());
                }
            }

            @Override
            public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                if (orderSet.contains(value.getTxId())){
                    orderSet.remove(value.getTxId());
                    out.collect("付款id"+value.getTxId());
                }else {
                    TxSet.add(value.getTxId());
                }
            }
        }).print();

        env.execute();

    }
}
