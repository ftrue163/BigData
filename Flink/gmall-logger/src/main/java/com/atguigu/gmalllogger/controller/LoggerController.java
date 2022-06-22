package com.atguigu.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class LoggerController {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("test")
    public String test() {
        System.out.println("1111");
        return "success";
    }

    @RequestMapping("test2")
    public String test2(@RequestParam("name") String name) {
        System.out.println(name);
        return "success";
    }

    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String jsonStr){

        //System.out.println(jsonStr);

        //写法1：
        //Logger logger = LoggerFactory.getLogger(LoggerController.class);
        //logger.info(jsonStr);

        //写法2  在此类上添加注解@Slf4j
        log.info(jsonStr);

        kafkaTemplate.send("ods_base_log", jsonStr);

        return "success";
    }
}
