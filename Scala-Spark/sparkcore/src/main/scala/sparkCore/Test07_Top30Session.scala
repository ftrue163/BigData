package sparkCore

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yhm
 * @create 2021-09-28 15:55
 */
object Test07_Top30Session {
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
    // 进行缓存  有利于第二次使用
    userVisitActionRDD.cache()


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
    val result1: Array[CategoryCountInfo] = result.sortBy(info => (info.clickCount, info.orderCount, info.payCount), false).take(10)


    //    result1.foreach(println)

    // 取出热门品类top10  作为一个集合
    val list: List[String] = result1.map(info => info.categoryId).toList

    // 将list作为广播变量
    val broList: Broadcast[List[String]] = sc.broadcast(list)

    // 将转换为categoryInfo对象的rdd进行过滤
    // 虽然结果对象中已经将品类id单独列出来了,  没有包含连接信息  所以没有办法使用
    //    val value: RDD[CategoryCountInfo] = cateCountInfoRDD.filter(info => broList.value.contains(info.categoryId))
    //    value.collect().foreach(println)

    // 过滤出top10的数据
    // 只判断点击的品类
    val filterRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action =>
      broList.value.contains(action.click_category_id))

    // 转换结构
    // ((点击品类,session),1)
    val cateSessionRDD: RDD[((String, String), Int)] = filterRDD.map(action => ((action.click_category_id, action.session_id), 1))

    // 聚合当前品类连接的活跃次数
    val cateSessionCountRDD: RDD[((String, String), Int)] = cateSessionRDD.reduceByKey(_ + _)

    // 转换结构 -> (id,(session,count))
    val sessionRDD: RDD[(String, (String, Int))] = cateSessionCountRDD.map({
      case (tuple, count) => (tuple._1, (tuple._2, count))
    })

    // 将相同品类的组聚合起来
    val cateGroupRDD: RDD[(String, Iterable[(String, Int)])] = sessionRDD.groupByKey()


    // 最终排序取不同品类的top10
    val sessionTop10: RDD[(String, List[(String, Int)])] = cateGroupRDD.mapValues(list => {
      list.toList.sortWith(_._2 > _._2).take(10)
    })

    //    sessionTop10.collect().foreach(println)


    // 4. 关闭sc
    sc.stop()
  }
}
