package com.atguigu.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ：less
 * @date ：Created in 2021/11/8 20:25
 * @description：
 * @modified By：
 * @version: $
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;
}
