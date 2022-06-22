package chapter07

/**
 * @author yhm
 * @create 2021-09-17 11:18
 */
object Test09_ListFunc {
  def main(args: Array[String]): Unit = {
    val list: List[Int] = List(1, 2, 3, 4, 5, 6, 7)
    val set = Set(1, 2, 3, 4)

    //    （1）获取集合长度
    val length: Int = list.length
    println(length)

    //    （2）获取集合大小
    val size: Int = set.size
    val size1: Int = list.size

    //    （3）循环遍历
    list.foreach(println)
    list.foreach( i => println(i * 2) )

    //    （4）迭代器
    val iterator: Iterator[Int] = list.iterator

    //    （5）生成字符串
    println(list.toString())

    val str: String = list.mkString("\t")
    val str1: String = list.mkString("list(","\t",")")
    println(str)
    println(str1)

    //    （6）是否包含
    val bool: Boolean = list.contains(1)
    val bool1: Boolean = list.contains(8)
    println(bool)
    println(bool1)

  }
}
