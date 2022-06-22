package com.atguigu.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-25 15:03
 */
object Test01_Serializable {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val list = List(new Person("zhangsan"), new Person("lisi"))

    val personRDD: RDD[Person] = sc.makeRDD(list, 2)

    personRDD.foreach(p => println(p.name))

    // 4. 关闭sc
    sc.stop()
  }

  // 主构造器参数
  // val 前缀 将name变为一个属性
  // 如果在spark中传递  需要进行序列化 不然报错
  // 1: 实现序列化接口
  //  class Person(val name:String) extends Serializable {
  //  }

  // 2: 使用样例类
  // 样例类中的参数  默认就是val的属性
  case class Person(name: String)

}
