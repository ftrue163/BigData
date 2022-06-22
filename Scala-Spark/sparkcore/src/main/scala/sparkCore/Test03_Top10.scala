package sparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-28 11:14
 */
object Test03_Top10 {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    // 读取数据源
    val lineRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")

    // 只对数据源处理一次 根据不同的类型直接统计为相同格式的数据
    // 如果是点击数据  -> (id,(1,0,0)
    // 如果是下单数据  -> (id,(0,1,0)
    // 如果是支付数据  -> (id,(0,0,1)

    // 一: 过滤掉不需要的数据
    val filterRDD: RDD[String] = lineRDD.filter(s => {
      val data: Array[String] = s.split("_")
      data(6) != "-1" || data(8) != "null" || data(10) != "null"
    })


    // 二: 转换格式为(id,(1,0,0)
    val flatMapRDD: RDD[(String, (Int, Int, Int))] = filterRDD.flatMap(s => {
      val data: Array[String] = s.split("_")
      if (data(6) != "-1") {
        // 点击数据
        List((data(6), (1, 0, 0)))
      } else if (data(8) != "null") {
        // 下单数据
        val orderList: Array[String] = data(8).split(",")
        orderList.map(id => (id, (0, 1, 0)))
      } else if (data(10) != "null") {
        // 支付数据
        val payList: Array[String] = data(10).split(",")
        payList.map(id => (id, (0, 0, 1)))
      } else
        Nil
    })

    // 三: 按照id聚合数据  统计三个次数
    val reduceRDD: RDD[(String, (Int, Int, Int))] = flatMapRDD.reduceByKey((res, elem) => (res._1 + elem._1, res._2 + elem._2, res._3 + elem._3))

    val result: Array[(String, (Int, Int, Int))] = reduceRDD.sortBy(_._2, false).take(10)

    result.foreach(println)

    Thread.sleep(600000)

    // 4. 关闭sc
    sc.stop()
  }
}
