package com.atguigu.gmallpublisher1.controller;

import com.atguigu.gmallpublisher1.service.ProductService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

//@Controller
@RestController
@RequestMapping("/api/sugar")
public class SugarController {
    @Autowired
    public ProductService productService;

    @RequestMapping("test")
    //@ResponseBody
    public String test1() {
        System.out.println("aaaaaa");
        return "success";
        //return "index.html";
    }

    @RequestMapping("test2")
    //@ResponseBody
    public String test2(@RequestParam("name") String nn, @RequestParam(value = "age", defaultValue = "18") int age) {
        System.out.println(nn + ":" + age);
        return "success";
    }

    @RequestMapping("/gmv")
    public String getGmv(@RequestParam(value = "date", defaultValue = "0") int date) {
        if (date == 0) {
            date = getToday();
        }
        BigDecimal gmv = productService.getGmv(date);

        return "{ " + gmv + "}";
    }

    @RequestMapping("/trademark")
    public String getGmvByTm(@RequestParam(value = "date", defaultValue = "0") int date, @RequestParam(value = "limit", defaultValue = "5") int limit) {
        if (date == 0) {
            date = getToday();
        }

        //查询数据
        Map gmvByTm = productService.getGmvByTm(date, limit);

        //取出Map的key与value
        Set keySet = gmvByTm.keySet();
        Collection values = gmvByTm.values();

        //拼接并返回数据
        return StringUtils.join(keySet, ',') + ":" + StringUtils.join(values, ',');
    }


    private int getToday() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        long ts = System.currentTimeMillis();

        return Integer.parseInt(sdf.format(ts));
    }






















}
