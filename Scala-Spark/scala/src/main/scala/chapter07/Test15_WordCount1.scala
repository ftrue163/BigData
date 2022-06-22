package chapter07

/**
 * @author yhm
 * @create 2021-09-18 10:25
 */
object Test15_WordCount1 {
  def main(args: Array[String]): Unit = {
    // 第一种方式（不通用）
    val tupleList = List(("Hello Scala Spark World", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))

    // 还原为长字符串
    val strings: List[String] = tupleList.map((tuple: (String, Int)) => (tuple._1 + " ") * tuple._2)
    println(strings)

    val tuples: List[(String, Int)] = strings.flatMap(_.split(" "))
      .groupBy(s => s)
      .mapValues(_.size)
      .toList
      .sortWith(_._2 > _._2)
      .take(3)

    println(tuples)


    // 步骤一: 修改格式为  (长字符串,次数) =>  List((单词,次数),(单词,次数))
    val list: List[List[(String, Int)]] = tupleList.map((tuple: (String, Int)) => {
      val strings1: Array[String] = tuple._1.split(" ")
      // Array(hello,scala,spark...)
      strings1.map((s: String) => (s, tuple._2)).toList
    })
    println(list)

    // 扁平化拆散
    val flatten: List[(String, Int)] = list.flatten
    println(flatten)

    // 使用flatMap进行简化
    val tuples1: List[(String, Int)] = tupleList.flatMap(tuple => tuple._1.split(" ").map((_, tuple._2)))

    println(tuples1)

    // 步骤二: 将相同的单词聚合
    val stringToTuples: Map[String, List[(String, Int)]] = tuples1.groupBy((tuple: (String, Int)) => tuple._1)
    val stringToTuples1: Map[String, List[(String, Int)]] = tuples1.groupBy(_._1)

    println(stringToTuples)

    // 步骤三: 相同的单词将次数相加
    val stringToInt: Map[String, Int] = stringToTuples.mapValues((list: List[(String, Int)]) =>
      list.map((tuple: (String, Int)) => tuple._2).sum)

    println(stringToInt)

    val stringToInt1: Map[String, Int] = stringToTuples.mapValues(_.map(_._2).sum)
    println(stringToInt1)

    // 步骤四: 排序取top3
    val result: List[(String, Int)] = stringToInt1.toList
      .sortWith(_._2 > _._2)
      .take(3)

    println(result)

  }
}
