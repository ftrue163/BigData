package chapter07

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * @author yhm
 * @create 2021-09-17 9:07
 */
object Test02_ArrayBuffer {
  def main(args: Array[String]): Unit = {
    // 可变数组
    // 默认使用的集合都是不可变的
    // 使用可变集合 需要自己提前导包
    val arrayBuffer: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    val arrayBuffer1: ArrayBuffer[Int] = ArrayBuffer(1, 2, 3, 4)


    // 向可变数组中添加元素
    arrayBuffer.append(10)
    arrayBuffer1.appendAll(Array(1,2,3,4))

    // 遍历打印
    arrayBuffer.foreach(println)
    arrayBuffer1.foreach(println)

    println(arrayBuffer1)


    // 修改元素
    arrayBuffer1.update(0,100)
    arrayBuffer1(1) = 200
    println(arrayBuffer1)

    // 查看元素
    println(arrayBuffer1(0))

    // 删除元素
    arrayBuffer1.remove(0)
    println(arrayBuffer1)
    arrayBuffer1.remove(1,3)
    println(arrayBuffer1)



    // 可变数组和不可变数组的转换和关系
    // 不可变
    val ints: Array[Int] = Array(1, 2, 3, 4)
    // 可变
    val ints1: ArrayBuffer[Int] = ArrayBuffer(5, 6, 7, 8)

    // 不可变的用符号
    val b: Array[Int] = ints :+ 1

    ints.foreach(println)
    b.foreach(println)

    // 可变的用方法
    ints1.append(1)
    println(ints1)

    val ints2: ArrayBuffer[Int] = ints1 :+ 2
    println(ints1)

    // 可变数组转换为不可变数组
    val array: Array[Int] = ints1.toArray
//    array.append

    // 不可变数组转可变数组
    // 结果用多态表示
    val buffer: mutable.Buffer[Int] = ints.toBuffer
    val buffer1: ArrayBuffer[Int] = buffer.asInstanceOf[ArrayBuffer[Int]]
    buffer.append(1)

  }
}
