package com.atguigu.bean;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class OrderInfo {
    Long id;
    Long province_id;
    String order_status;
    Long user_id;
    BigDecimal total_amount;
    BigDecimal activity_reduce_amount;
    BigDecimal coupon_reduce_amount;
    BigDecimal original_total_amount;
    BigDecimal weight_fee;
    String expire_time;
    String create_time;
    String operate_time;
    //下面3个字段 通过解析create_time字段获得
    String create_date;
    String create_hour;
    Long create_ts;
}
