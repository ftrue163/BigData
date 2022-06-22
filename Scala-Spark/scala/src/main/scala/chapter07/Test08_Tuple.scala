package chapter07

/**
 * @author yhm
 * @create 2021-09-17 11:08
 */
object Test08_Tuple {
  def main(args: Array[String]): Unit = {
    // 创建元组
    val tuple = new Tuple1[Int](10)
    println(tuple)

    val tuple1 = new Tuple2[String, Int]("hello", 10)
    println(tuple1)

    val tuple2: (String, Int) = ("linhai", 18)


    // 调用元组的元素
    // 元组的长度和元素值都是决定不能够修改的
    val value: String = tuple2._1
    val value1: Int = tuple2._2
    tuple.productElement(0)


    // 元组和map的关系
    val map1 = Map(("hello", 1), ("world", 2))

    for (elem <- map1) {
      // map中的元素本质上就是二元组
      elem
    }

  }
}
