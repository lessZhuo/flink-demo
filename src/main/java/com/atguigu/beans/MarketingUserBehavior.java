package com.atguigu.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ：less
 * @date ：Created in 2021/11/9 19:27
 * @description：
 * @modified By：
 * @version: $
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MarketingUserBehavior {
    private Long userId;
    private String behavior;
    private String channel;
    private Long timestamp;
}
