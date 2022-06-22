package com.atguigu.gmallpublisher1.service.impl;

import com.atguigu.gmallpublisher1.mapper.ProductMapper;
import com.atguigu.gmallpublisher1.service.ProductService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ProductServiceImpl implements ProductService {
    @Autowired
    public ProductMapper productMapper;

    @Override
    public BigDecimal getGmv(int date) {
        return productMapper.selectGmv(date);
    }

    @Override
    public Map getGmvByTm(int date, int limit) {
        //查询ClickHouse获取品牌GMV数据
        List<Map> mapList = productMapper.selectGmvByTm(date, limit);

        //创建Map用于存放最终结果数据
        HashMap resultMap = new HashMap<>();

        //遍历mapList，将数据取出放入resultMap
        for (Map map : mapList) {
            Object tm_name = map.get("tm_name");
            Object total_order_amount = map.get("total_order_amount");
            resultMap.put((String) tm_name, (BigDecimal) total_order_amount);
        }

        //返回结果集
        return resultMap;
    }
}
