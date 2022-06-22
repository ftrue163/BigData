package sparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-28 10:01
 */
object Test02_Top10 {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    // 需求: top10的热门品牌
    // 读取数据源
    val lineRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")

    // LINERDD后续使用多次 先进行缓存效率更高
    lineRDD.cache()

    // 一: 统计不同品类的点击次数
    // 过滤出点击的数据
    val clickLineRDD: RDD[String] = lineRDD.filter(s => {
      // 注意这里为字符串  需要判断是不等于字符串的-1
      s.split("_")(6) != "-1"
    })

    // 保留需要的数据即可
    val value: RDD[(String, Int)] = clickLineRDD.map(s => {
      val clickCategory: String = s.split("_")(6)
      (clickCategory, 1)
    })

    val clickCountRDD: RDD[(String, Int)] = value.reduceByKey(_ + _)
    //    clickCountRDD.collect().foreach(println)


    // 二: 下单品类的次数
    // 过滤出下单的数据
    val orderLineRDD: RDD[String] = lineRDD.filter(s => {
      s.split("_")(8) != "null"
    })

    // 保留下单的数据
    val value1: RDD[String] = orderLineRDD.map(s => s.split("_")(8))

    val value2: RDD[String] = value1.flatMap(s => s.split(","))

    // 合并上述两个map

    val value3: RDD[String] = orderLineRDD.flatMap(s => {
      val data: String = s.split("_")(8)
      data.split(",")
    })

    val orderCountRDD: RDD[(String, Int)] = value2
      .map((_, 1))
      .reduceByKey(_ + _)

    //    orderCountRDD.collect().foreach(println)


    // 三: 支付品类的次数
    // 过滤出支付相关的数据
    val payLineRDD: RDD[String] = lineRDD.filter(s => s.split("_")(10) != "null")

    // 拆分出支付的品类
    val value4: RDD[String] = payLineRDD.flatMap(s => {
      val data: String = s.split("_")(10)
      data.split(",")
    })

    val payCountRDD: RDD[(String, Int)] = value4.map((_, 1))
      .reduceByKey(_ + _)

    //    payCountRDD.collect().foreach(println)


    // 四: 转换数据格式 将三个count的数据按照品类id合并在一起
    // 点击  (id,count)  ->  (id,("click",count)) -> (id,(count,0,0)
    // 下单  (id,count)  ->  (id,("order",count)) -> (id,(0,count,0)
    // 支付  (id,count)  ->  (id,("pay",count))   -> (id,(0,0,count)
    // 通过使用位置来标记属于什么类型的count数  在进行统计的时候 更加便捷
    val clickRDD: RDD[(String, (Int, Int, Int))] = clickCountRDD.map({
      case (id, count) => (id, (count, 0, 0))
    })

    val orderRDD: RDD[(String, (Int, Int, Int))] = orderCountRDD.map({
      case (id, count) => (id, (0, count, 0))
    })

    val payRDD: RDD[(String, (Int, Int, Int))] = payCountRDD.map({
      case (id, count) => (id, (0, 0, count))
    })


    val categoryRDD: RDD[(String, (Int, Int, Int))] = clickRDD.union(orderRDD).union(payRDD)

    // 使用reduceByKey代替groupBy  效率更高
    val cateTupleRDD: RDD[(String, (Int, Int, Int))] = categoryRDD.reduceByKey((tuple1, tuple2) => {
      (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2, tuple1._3 + tuple2._3)
    })

    val result: Array[(String, (Int, Int, Int))] = cateTupleRDD.sortBy(_._2, false).take(10)

    result.foreach(println)

    Thread.sleep(600000)

    // 4. 关闭sc
    sc.stop()
  }
}
