package chapter07

import scala.collection.mutable.ListBuffer

/**
 * @author yhm
 * @create 2021-09-17 10:12
 */
object Test05_ListBuffer {
  def main(args: Array[String]): Unit = {
    // 可变list创建
    val listBuffer = new ListBuffer[Int]()

    val listBuffer1: ListBuffer[Int] = ListBuffer(1, 2, 3, 4)

    // 增加元素
    listBuffer1.append(5)
    listBuffer1.prepend(0)

    println(listBuffer1)

    // 删除元素
    listBuffer1.remove(0)
    println(listBuffer1)

    // 查看修改
    listBuffer1(0) = 1


  }
}
