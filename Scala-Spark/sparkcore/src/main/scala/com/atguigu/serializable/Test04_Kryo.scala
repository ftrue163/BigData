package com.atguigu.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-25 15:34
 */
object Test04_Kryo {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")
      // 替换默认的序列化机制
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注册需要使用kryo序列化的自定义类
      // 填入的参数是  类模板的数组
      .registerKryoClasses(Array(classOf[Person],classOf[Person1]))


    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val rdd: RDD[AnyRef] = sc.makeRDD(List(new Person("zhangsan"), new Person1("lisi")), 2)

    rdd.foreach(p => println(p))

    // 4. 关闭sc
    sc.stop()
  }

  // 默认实现序列化使用的是 java自带的序列化
  class Person(val name:String) extends Serializable

  class Person1(val name:String) extends Serializable
}
