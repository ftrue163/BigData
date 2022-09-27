package com.atguigu.gmall.realtime.util

import java.util.ResourceBundle

/**
 * 配置文件解析类
 */
object MyPropsUtils {

    private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

    def apply(propsKey: String): String = {
        bundle.getString(propsKey)
    }

    // 测试MyPropsUtils的功能
    def main(args: Array[String]): Unit = {
        println(MyPropsUtils("kafka.bootstrap-servers"))
        println(MyPropsUtils("redis.host"))
        println(MyPropsUtils("redis.port"))
    }
}
