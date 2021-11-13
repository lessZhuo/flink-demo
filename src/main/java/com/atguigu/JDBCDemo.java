package com.atguigu;

import com.atguigu.beans.MywaterSensor;
import com.atguigu.beans.WaterSensor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author ：less
 * @date ：Created in 2021/11/9 16:35
 * @description：
 * @modified By：
 * @version: $
 */
public class JDBCDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> wsDSS = env.addSource(new MywaterSensor());

        wsDSS.addSink(JdbcSink.sink("replace into sensor (`id`,`ts`,`vc`) values(?,?,?)",
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                            preparedStatement.setString(1,waterSensor.getId());
                            preparedStatement.setLong(2,waterSensor.getTs());
                            preparedStatement.setInt(3,waterSensor.getVc());
                    }
                }, JdbcExecutionOptions.builder().withBatchSize(100).withMaxRetries(5).withBatchIntervalMs(100).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl("jdbc:mysql://hadoop102:3306/DEFAULT?useSSL=false")
                .withUsername("root").withPassword("000000").withDriverName("com.mysql.jdbc.Driver").build()
        ));


//        wsDSS.addSink(
//                JdbcSink
//                        /**
//                         * 使用replace into防止有相同的主键的值插入时候出错
//                         * 1. 根据主键或者唯一索引判断，如果发现表中已经有此行数据，则先删除此行数据，然后插入新的数据。
//                         * 2. 否则，直接插入新数据。
//                         */
//                        .sink("replace into sensor (`id`,`ts`,`vc`) values(?,?,?)",
//                                new JdbcStatementBuilder<WaterSensor>() {
//                                    @Override
//                                    public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
//                                        preparedStatement.setString(1, waterSensor.getId());
//                                        preparedStatement.setLong(2, waterSensor.getTs());
//                                        preparedStatement.setInt(3, waterSensor.getVc());
//                                    }
//                                },
//                                // (statement, r) -> {
//                                //     statement.setString(1, r.getId());
//                                //     statement.setLong(2, r.getTs());
//                                //     statement.setInt(3, r.getVc());
//                                // },
//                                JdbcExecutionOptions
//                                        .builder()
//                                        .withMaxRetries(5)
//                                        .withBatchSize(1000)
//                                        .withBatchIntervalMs(200)
//                                        .build(),
//                                new JdbcConnectionOptions
//                                        .JdbcConnectionOptionsBuilder()
//                                        // url链接中如果不添加useSSL=false的话会有如下警告信息
//                                        /**
//                                         * WARN: Establishing SSL connection without server's identity verification is not recommended.
//                                         * According to MySQL 5.5.45+, 5.6.26+ and 5.7.6+ requirements SSL connection
//                                         * must be established by default if explicit option isn't set.
//                                         * For compliance with existing applications not using SSL the verifyServerCertificate property is set to 'false'.
//                                         * You need either to explicitly disable SSL by setting useSSL=false,
//                                         * or set useSSL=true and provide truststore for server certificate verification.
//                                         */
//                                        .withUrl("jdbc:mysql://hadoop102/DEFAULT?useSSL=false")
//                                        .withUsername("root")
//                                        .withPassword("000000")
//                                        // 驱动名一定要写成com.mysql.jdbc.Driver，不能有任何大小写错误！
//                                        // 使用MySQL 5.7的话，驱动名中没有cj，版本6之后的驱动名为com.mysql.cj.jdbc.Driver
//                                        .withDriverName("com.mysql.jdbc.Driver")
//                                        .build()
//                        ));
        env.execute();


    }
}
