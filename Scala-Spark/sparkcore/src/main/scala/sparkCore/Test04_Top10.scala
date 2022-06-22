package sparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-28 11:34
 */
object Test04_Top10 {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    // 读取数据源
    val lineRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")

    // 将lineRDD转换为样例类
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


    // 将原始数据的样例类转换为结果数据的样例类
    val cateCountInfoRDD: RDD[CategoryCountInfo] = userVisitActionRDD.flatMap(action => {
      if (action.click_category_id != "-1") {
        // 点击数据
        List(CategoryCountInfo(action.click_category_id, 1, 0, 0))
      } else if (action.order_category_ids != "null") {
        // 下单数据
        val ids: Array[String] = action.order_category_ids.split(",")
        ids.map(id => CategoryCountInfo(id, 0, 1, 0))
      } else if (action.pay_category_ids != "null") {
        // 支付数据
        val ids: Array[String] = action.pay_category_ids.split(",")
        ids.map(id => CategoryCountInfo(id, 0, 0, 1))
      } else Nil
    })

    // 将相同的品类聚合在一起
    val idListRDD: RDD[(String, Iterable[CategoryCountInfo])] = cateCountInfoRDD.groupBy(_.categoryId)


    val result: RDD[CategoryCountInfo] = idListRDD.map({
      case (id, list) => list.reduce((res, elem) => {
        res.clickCount += elem.clickCount
        res.orderCount += elem.orderCount
        res.payCount += elem.payCount
        res
      })
    })

    // 排序取top10
    val result1: Array[CategoryCountInfo] = result.sortBy(info => (info.clickCount, info.orderCount, info.payCount),false).take(10)

    result1.foreach(println)


    // 4. 关闭sc
    sc.stop()
  }
}
