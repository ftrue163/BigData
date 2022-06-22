package chapter07

import scala.collection.mutable

/**
 * @author yhm
 * @create 2021-09-17 10:50
 */
object Test07_Map {
  def main(args: Array[String]): Unit = {
    // 默认使用不可变map
    val map: Map[String, Int] = Map("hello" -> 1, "world" -> 2)
    val map1 = Map(("hello", 1), ("world", 2))

    // 遍历打印map
    for (elem <- map) {
      println(elem)
    }

    map.foreach(println)

    val keys: Iterable[String] = map.keys
    keys.foreach(println)

    val values: Iterable[Int] = map.values

    // 直接打印map
    println(map)


    // key是无序不可重复的
    val map2 = Map( ("z", 3),("a", 1), ("a", 2), ("c", 3),("f",4),("d",5))
    println(map2)


    // 获取value的值
    val option: Option[Int] = map2.get("a")
    println(option)


    if (!map2.get("m").isEmpty) {
      val value: Int = map2.get("m").get
    }

    // option有区分是否有数据的方法 使用getOrElse  如果为None  去默认值
    option.getOrElse(1)


    // 如果不确认存在
    val i: Int = map2.getOrElse("m", 10)
    // 如果确认存在的话
    val i1: Int = map2("a")

    // 可变map
    val map3: mutable.Map[String, Int] = mutable.Map(("z", 3), ("a", 1), ("a", 2), ("c", 3), ("f", 4), ("d", 5))

    // 可变map可以使用put方法放入元素
    map3.put("z",10)
    println(map3)

    // 修改元素的方法
    map3.update("z",20)
    map3("z") = 30

  }
}
