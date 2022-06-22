package sparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

/**
 * @author yhm
 * @create 2021-09-28 14:21
 */
object Test06_Top10 {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")

    val userVisitActionRDD: RDD[UserVisitAction] = lineRDD.map(s => {
      val data: Array[String] = s.split("_")
      new UserVisitAction(
        data(0),
        data(1),
        data(2),
        data(3),
        data(4),
        data(5),
        data(6),
        data(7),
        data(8),
        data(9),
        data(10),
        data(11),
        data(12)
      )
    })

    // 创建自己的累加器
    val acc = new MyAcc
    sc.register(acc)

    // 使用累加器
    userVisitActionRDD.foreach(action => {
      acc.add(action)
    })

    // 调用累加器的结果
    // ((id,"click"),count)
    val map: mutable.Map[(String, String), Long] = acc.value


    // 按照id进行聚合
    val map1: Map[String, mutable.Map[(String, String), Long]] = map.groupBy(_._1._1)

    // 转换格式为categoryInfo
    val categoryCountInfoes: immutable.Iterable[CategoryCountInfo] = map1.map({
      case (id, list) =>
        val clickCount: Long = list.getOrElse((id, "click"), 0L)
        val orderCount: Long = list.getOrElse((id, "order"), 0L)
        val payCount: Long = list.getOrElse((id, "pay"), 0)
        CategoryCountInfo(id, clickCount, orderCount, payCount)
    })

    val result: List[CategoryCountInfo] = categoryCountInfoes.toList
      .sortBy(info => (info.clickCount, info.orderCount, info.payCount))(Ordering[(Long,Long,Long)].reverse)
      .take(10)

    result.foreach(println)

    Thread.sleep(600000)

    // 4. 关闭sc
    sc.stop()
  }

  class MyAcc extends AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] {

    val map: mutable.Map[(String, String), Long] = mutable.Map[(String, String), Long]()

    override def isZero: Boolean = map.isEmpty

    override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = new MyAcc

    override def reset(): Unit = map.clear()

    override def add(v: UserVisitAction): Unit = {

      if (v.click_category_id != "-1") {
        val clickKey: (String, String) = (v.click_category_id, "click")
        map(clickKey) = map.getOrElse(clickKey, 0L) + 1L
      }
      else if (v.order_category_ids != "null") {
        val orderIds: Array[String] = v.order_category_ids.split(",")

        orderIds.foreach(id => {
          val orderKey: (String, String) = (id, "order")
          map(orderKey) = map.getOrElse(orderKey, 0L) + 1L
        })
      }
      else if (v.pay_category_ids != "null") {
        val payIds: Array[String] = v.pay_category_ids.split(",")

        payIds.foreach(id => {
          val payKey: (String, String) = (id, "pay")
          map(payKey) = map.getOrElse(payKey, 0L) + 1L
        })
      }
    }

    override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
      // 合并两个map
      val map1: mutable.Map[(String, String), Long] = other.value

      for (elem <- map1) {
        map(elem._1) = map.getOrElse(elem._1, 0L) + elem._2
      }


    }

    override def value: mutable.Map[(String, String), Long] = map
  }

}
