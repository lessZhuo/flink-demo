package com.atguigu.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ：less
 * @date ：Created in 2021/11/9 18:57
 * @description：
 * @modified By：
 * @version: $
 */
@AllArgsConstructor
@Data
@NoArgsConstructor
public class UserBehavior {
    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;
}
