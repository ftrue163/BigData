package com.atguigu.gmall.realtime.test.util

import java.util.ResourceBundle

/**
 * 配置文件解析类
 */
object PropertiesUtils {

    private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

    def apply(key: String): String = {
        bundle.getString(key)
    }

    def main(args: Array[String]): Unit = {
        println(PropertiesUtils("kafka.broker.list"))
    }
}
