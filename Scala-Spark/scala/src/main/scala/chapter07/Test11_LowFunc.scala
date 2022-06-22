package chapter07

import scala.collection.mutable.ListBuffer

/**
 * @author yhm
 * @create 2021-09-17 14:22
 */
object Test11_LowFunc {
  def main(args: Array[String]): Unit = {
    val list: List[Int] = List(1, 5, -3, 4, 2, -7, 6)
    val list1: ListBuffer[Int] = ListBuffer(1, 5, -3, 4, 2, -7, 6)

    //    （1）求和
    val sum: Int = list.sum
    println(sum)

    //    （2）求乘积
    val product: Int = list.product
    println(product)

    //    （3）最大值
    val max: Int = list.max

    //    （4）最小值
    val min: Int = list.min

    //    （5）排序
    val sorted: List[Int] = list.sorted
    println(list)
    println(sorted)

    // 修改排序规则 从大到小
    val ints: List[Int] = list.sorted(Ordering[Int].reverse)
    println(ints)

    // 对元组进行排序
    val tuples = List(("hello", 10), ("world", 2), ("scala", 9), ("haha", 4),("hello", 1))

    // 按照元组的默认字典序排列
    val sorted1: List[(String, Int)] = tuples.sorted
    println(sorted1)

    // 按照后面数字从小到大排序
    val tuples1: List[(String, Int)] = tuples.sortBy((tuple: (String, Int)) => tuple._2)
    println(tuples1)

    // 按照后面数字从大到小排序
    val tuples2: List[(String, Int)] = tuples.sortBy((tuple: (String, Int)) => tuple._2)(Ordering[Int].reverse)
    println(tuples2)

    tuples.sortBy( _._2 )

    // 自定义排序规则
    val tuples3: List[(String, Int)] = tuples.sortWith((left: (String, Int), right: (String, Int)) => left._2 > right._2)
    println(tuples3)


    val tuples4: List[(String, Int)] = tuples.sortWith(_._2 > _._2)
    println(tuples4)

  }
}
