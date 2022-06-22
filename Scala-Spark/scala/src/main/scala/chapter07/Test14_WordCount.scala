package chapter07

/**
 * @author yhm
 * @create 2021-09-18 9:35
 */
object Test14_WordCount {
  def main(args: Array[String]): Unit = {
    // 单词计数：将集合中出现的相同的单词，进行计数，取计数排名前三的结果
    val stringList = List("Hello Scala Hbase kafka", "Hello Scala Hbase", "Hello Scala", "Hello")

    // 步骤一: 扁平化拆分单词

    // str => Array(单词,单词)
    val list: List[List[String]] = stringList.map((s: String) => s.split(" ").toList)
    println(list)

    // 扁平化拆分
    val flatten: List[String] = list.flatten
    println(flatten)

    // 合并使用flatmap
    val list1: List[String] = stringList.flatMap((s: String) => s.split(" "))
    println(list1)

    val flatMap: List[String] = stringList.flatMap(_.split(" "))

    // 步骤二: 使用分组聚合
    // 结果为map  (单词,List(单词,单词,单词))
    val wordMap: Map[String, List[String]] = flatMap.groupBy((s: String) => s)
    val map1: Map[String, List[String]] = flatMap.groupBy( (s) => s)
    println(wordMap)

    // 步骤三: 转换结果为  (单词,次数)

    val wordCountMap: Map[String, Int] = wordMap.map((tuple: (String, List[String])) => (tuple._1, tuple._2.size))
    println(wordCountMap.size == wordMap.size)
    println(wordCountMap)

    wordMap.map((tuple) => (tuple._1, tuple._2.size))

    // 如果原先的集合本身就是kv结果  并且转换的时候  key保持不变 可以使用mapValues进行简化
    val wordCountMap1: Map[String, Int] = wordMap.mapValues((list: List[String]) => list.size)
    val wordCountMap2: Map[String, Int] = wordMap.mapValues(_.size)
    println(wordCountMap1)

    // 步骤四: 排序取top3
    // 安装单词出现的次数从大到小排序
    val list2: List[(String, Int)] = wordCountMap2.toList
    val tuples: List[(String, Int)] = list2.sortWith((left: (String, Int), right: (String, Int)) => left._2 > right._2)
    val tuples1: List[(String, Int)] = list2.sortWith(_._2 > _._2)

    println(tuples)

    val result: List[(String, Int)] = tuples.take(3)
    println(result)

    // 熟练的写法
    val result1: List[(String, Int)] = stringList.flatMap(_.split(" "))
      .groupBy(s => s)
      .mapValues(_.size)
      .toList
      .sortWith(_._2 > _._2)
      .take(3)

    println(result1)

  }
}
