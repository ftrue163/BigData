package chapter07

import scala.collection.mutable

/**
 * @author yhm
 * @create 2021-09-17 16:23
 */
object Test13_ReduceMap {
  def main(args: Array[String]): Unit = {
    // 需求: 归约两个map , 如果另外一个map中不存在key,放入当前map,如果存在,将两个value值相加
    val map: mutable.Map[String, Int] = mutable.Map(("hello", 10), ("scala", 8))
    val map1: mutable.Map[String, Int] = mutable.Map(("haha", 10), ("scala", 8), ("world", 20))


    //    for (elem <- map1) {
    //      if (map.contains(elem._1)){
    //        // 如果map中已经存在对应的元素
    //        map.update(elem._1,elem._2 + map.getOrElse(elem._1,0))
    //      }else{
    //        // 如果map中不存在对应的元素
    //        map.put(elem._1,elem._2)
    //      }
    //    }


    //    for (elem <- map1) {
    //      map.put(elem._1,elem._2 + map.getOrElse(elem._1,0))
    //    }

    // 使用伴生类的apply方法
    //    for (elem <- map1) {
    //      map(elem._1) = elem._2 + map.getOrElse(elem._1,0)
    //    }

    // 使用高阶函数完成归约两个map
    val map2: mutable.Map[String, Int] = map1.foldLeft(map)((res: mutable.Map[String, Int], elem: (String, Int)) => {
      res(elem._1) = elem._2 + res.getOrElse(elem._1, 0)
      res
    })


    println(map)
    println(map2)

  }
}
