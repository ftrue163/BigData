package chapter07

/**
 * @author yhm
 * @create 2021-09-17 14:03
 */
object Test10_ListFunc1 {
  def main(args: Array[String]): Unit = {
    val list1: List[Int] = List(1, 2, 3, 4, 5, 6, 7)
    val list2: List[Int] = List(4, 5, 6, 7, 8, 9, 10)
    val list3 = List()


    //    （1）获取集合的头
    val head: Int = list1.head
    //    list3.head
    //    list3(0)
    println(head)

    //    （2）获取集合的尾（不是头的就是尾）
    val tail: List[Int] = list1.tail
    println(tail)

    //    （3）集合最后一个数据
    val last: Int = list1.last
    println(last)

    //    （4）集合初始数据（不包含最后一个）
    val init: List[Int] = list1.init
    println(init)

    //    （5）反转
    val reverse: List[Int] = list1.reverse

    //    （6）取前（后）n个元素
    val ints: List[Int] = list1.take(3)
    println(ints)
    val ints1: List[Int] = list1.takeRight(3)
    println(ints1)

    //    （7）去掉前（后）n个元素
    val ints2: List[Int] = list1.drop(3)
    println(ints2)

    val ints3: List[Int] = list1.dropRight(3)
    println(ints3)

    //    （8）并集
    val ints4: List[Int] = list1.union(list2)
    println(ints4)

    //    （9）交集
    val ints5: List[Int] = list1.intersect(list2)
    println(ints5)

    //    （10）差集
    val ints6: List[Int] = list1.diff(list2)
    println(ints6)
    val ints7: List[Int] = list2.diff(list1)
    println(ints7)

    //    （11）拉链
    val tuples: List[(Int, Int)] = list1.zip(list2)
    println(tuples)

    val list = List(1, 2, 3, 4)
    val tuples1: List[(Int, Int)] = list1.zip(list)
    println(tuples1)

    //    （12）滑窗
    val iterator: Iterator[List[Int]] = list1.sliding(3)

    val iterator1: Iterator[List[Int]] = list1.sliding(4, 2)
    println(iterator1.toList)


  }
}
