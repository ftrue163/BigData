package com.atguigu.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * 工具类: 是用来读取配置文件的
 */
object PropertiesUtil {
    def load(propertiesName: String): Properties = {
        val prop = new Properties()
        prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName), "UTF-8"))
        prop
    }

    //测试PropertiesUtil工具类
    def main(args: Array[String]): Unit = {
        val prop = PropertiesUtil.load("config.properties")
        val str = prop.getProperty("kafka.broker.list")
        println(str)
    }
}
