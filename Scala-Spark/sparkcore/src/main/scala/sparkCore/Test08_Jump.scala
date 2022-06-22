package sparkCore

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author yhm
 * @create 2021-09-28 18:03
 */
object Test08_Jump {
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

    // 进行缓存  利于多次使用
    userVisitActionRDD.cache()
    
    // 求分母
    val list = List("1", "2", "3", "4", "5", "6", "7")
    val denominator: Broadcast[List[String]] = sc.broadcast(list)

    // 分子的广播变量
    val list1: List[(String, String)] = list.zip(list.tail)
    val molecule: Broadcast[List[(String, String)]] = sc.broadcast(list1)


    // 过滤出对应页面的数据
    val filterDenoRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => {
      denominator.value.contains(action.page_id)
    })

    val pageCountDenoRDD: RDD[(String, Int)] = filterDenoRDD.map(action => (action.page_id, 1))
      .reduceByKey(_ + _)




    // 计算分子
    // ((1,2),count)
    // 聚合同一个连接的数据
    val sessionTimeRDD: RDD[(String, (String, String))] = userVisitActionRDD.map(action =>
      (action.session_id, (action.action_time, action.page_id)))

    val sessionGroupRDD: RDD[(String, Iterable[(String, String)])] = sessionTimeRDD.groupByKey()


    // 对页面的时间进行排序
    // 转换格式  (page1,page2) => (1-2,2-3) => ((1,2),(2,3))
    val pageJumpRDD: RDD[(String, List[(String, String)])] =sessionGroupRDD.mapValues(list => {
      val pageJump: List[String] = list.toList.sortBy(_._1).map(_._2)
      // 元素类型(页面1,页面2)
      val tuples: List[(String, String)] = pageJump.zip(pageJump.tail)
      tuples
    })

    // 打散一个连接的跳转页面
    val pageToPageRDD: RDD[(String, String)] = pageJumpRDD.flatMap(_._2)

    // 将需要统计的分子进行过滤
    val pageToPageOneRDD: RDD[(String, String)] = pageToPageRDD.filter(tuple => molecule.value.contains(tuple))

    // 聚合相同的分子
    val moleculeRDD: RDD[((String, String), Int)] = pageToPageOneRDD.map((_, 1))
      .reduceByKey(_ + _)

    // 最终分子的结果
    val tuples: Array[((String, String), Int)] = moleculeRDD.collect()

    // 计算处理的分母结果
    val denoArray: Array[(String, Int)] = pageCountDenoRDD.collect()

    // 创建一个map收集单跳转换率的结果
    val result: mutable.Map[(String, String), Double] = mutable.Map[(String, String), Double]()


    val demoMap: Map[String, Int] = denoArray.toMap
    // 计算结果
    for (elem <- tuples) {
      result(elem._1) = (elem._2.toDouble / demoMap.getOrElse(elem._1._1,1))
    }

    result.foreach(println)

    
    // 4. 关闭sc
    sc.stop()
  }
}
