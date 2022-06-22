package com.atguigu.gmallpublisher1;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.gmallpublisher1.mapper")
public class GmallPublisher1Application {

    public static void main(String[] args) {
        SpringApplication.run(GmallPublisher1Application.class, args);
    }

}
