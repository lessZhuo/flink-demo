package com.atguigu.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ：less
 * @date ：Created in 2021/11/9 19:56
 * @description：
 * @modified By：
 * @version: $
 */
@AllArgsConstructor
@Data
@NoArgsConstructor
public class OrderEvent {
    private Long orderId;
    private String eventType;
    private String txId;
    private Long eventTime;
}
