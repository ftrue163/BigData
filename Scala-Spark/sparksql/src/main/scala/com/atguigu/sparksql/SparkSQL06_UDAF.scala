package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}


/**
 * @author layne
 */
object SparkSQL06_UDAF {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkSQLTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sparksession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("D:\\IdeaProjects\\SparkSQL0625\\input\\user.json")

    //sql风格语法
    df.createTempView("user")

    //自定义udaf,注册一下
    spark.udf.register("myAvg",functions.udaf(new MyAvgUDAF))


    spark.sql("select myAvg(age) from user").show()


    //TODO 3 关闭资源
    spark.stop()
  }

}

case class Buff(var sum:Long,var cnt:Long)
/**
 * 输入:Long
 * 缓存区:Buff
 * 输出:Double
 */
class MyAvgUDAF extends Aggregator[Long,Buff,Double]{
  //缓存区初始化方法
  override def zero: Buff = Buff(0L,0L)

  //缓存区在分区内的聚合方法
  override def reduce(buff: Buff, age: Long): Buff = {
    buff.sum += age
    buff.cnt += 1
    buff
  }

  //多个缓存区在分区间的聚合方法
  override def merge(b1: Buff, b2: Buff): Buff = {
    b1.sum += b2.sum
    b1.cnt += b2.cnt
    b1
  }

  //最终的逻辑计算方法(求平均值的方法)
  override def finish(buff: Buff): Double = {
    buff.sum.toDouble / buff.cnt
  }

  //buff和最终输出结果的序列化方法
  override def bufferEncoder: Encoder[Buff] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
