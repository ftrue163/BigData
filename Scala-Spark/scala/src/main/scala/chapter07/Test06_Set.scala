package chapter07

import scala.collection.immutable.HashSet
import scala.collection.mutable

/**
 * @author yhm
 * @create 2021-09-17 10:17
 */
object Test06_Set {
  def main(args: Array[String]): Unit = {
    // 创建set  使用伴生对象的apply方法

    val set: Set[Int] = Set(4, 3, 2, 1)
    val set1 = Set(1, 2, 3, 4, 2, 8, 4, 3, 7)

    // set的特点 无序不可重复
    println(set)

    // 默认使用hash set
    // 如果元素少于等于4个  会创建特定类型的set
    println(set.isInstanceOf[HashSet[Int]])

    val hashSet: HashSet[Int] = HashSet(1, 2, 3, 4, 5)

    // 不可变使用符号
    val set2: Set[Int] = set + 1
    println(set2)

    // 作用 判断集合是否包含某个元素
    val bool: Boolean = set.contains(2)


    // 可变的set
    val set3: mutable.Set[Int] = mutable.Set(1, 2, 3, 4, 4, 3, 2, 1)

    // 同样数据不可重复且无序
    println(set3)

    // 会使用返回值来告诉你有没有加入进去
    val bool1: Boolean = set3.add(5)
    println(set3)

    // 遍历查询set
    set3.foreach(println)

    // 删除元素  填写的不是下标是删除的元素
    val bool2: Boolean = set3.remove(3)
    println(set3)


  }
}
