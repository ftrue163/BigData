package sparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-28 10:01
 */
object Test01_Top10 {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    // 需求: top10的热门品牌
    // 读取数据源
    val lineRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")

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
    // 点击  (id,count)  ->  (id,("click",count))
    // 下单  (id,count)  ->  (id,("order",count))
    // 支付  (id,count)  ->  (id,("pay",count))
    // 使用分组集合  groupByKey  ->  (id,List( ("click",count),("order",count),("pay",count)   ))

    val clickRDD: RDD[(String, (String, Int))] = clickCountRDD.map({
      case (id, count) => (id, ("click", count))
    })

    val oderRDD: RDD[(String, (String, Int))] = orderCountRDD.map({
      case (id, count) => (id, ("order", count))
    })

    val payRDD: RDD[(String, (String, Int))] = payCountRDD.map({
      case (id, count) => (id, ("pay", count))
    })
    val categoryRDD: RDD[(String, (String, Int))] = clickRDD.union(oderRDD).union(payRDD)

//    categoryRDD.collect().foreach(println)

    // 聚合相同品类的rdd
    val cateGroupRDD: RDD[(String, Iterable[(String, Int)])] = categoryRDD.groupByKey()

    // 变化value结构为三元组
    val cateTupleRDD: RDD[(String, (Int, Int, Int))] = cateGroupRDD.mapValues(list => {
      var click = 0
      var order = 0
      var pay = 0
      for (elem <- list) {
        if (elem._1 == "click") {
          click = elem._2
        } else if (elem._1 == "order") {
          order = elem._2
        } else if (elem._1 == "pay") {
          pay = elem._2
        }
      }
      (click, order, pay)
    })

    // 排序取top10
    val tuples: Array[(String, (Int, Int, Int))] = cateTupleRDD.sortBy(_._2,false).take(10)

    tuples.foreach(println)

    Thread.sleep(600000)

    // 4. 关闭sc
    sc.stop()
  }
}
