package com.atguigu.gmallpublisher1.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;


public interface ProductMapper {
    @Select("select sum(order_amount) from product_stats_210625 where toYYYYMMDD(stt) = ${date}")
    BigDecimal selectGmv(int date);

    //数据的封装类型可以是List<Map>   也可以是List<JavaBean>
    @Select("select  " +
            "tm_name,  " +
            "sum(order_amount) total_order_amount  " +
            "from product_stats_210625  " +
            "where toYYYYMMDD(stt) = ${date} " +
            "group by tm_name " +
            "order by total_order_amount desc " +
            "limit ${limit}")
    List<Map> selectGmvByTm(@Param("date") int date, @Param("limit") int limit);
}
