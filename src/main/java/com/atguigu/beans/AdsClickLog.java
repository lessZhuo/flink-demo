package com.atguigu.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ：less
 * @date ：Created in 2021/11/9 19:40
 * @description：
 * @modified By：
 * @version: $
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AdsClickLog {
    private Long userId;
    private Long adId;
    private String province;
    private String city;
    private Long timestamp;
}
