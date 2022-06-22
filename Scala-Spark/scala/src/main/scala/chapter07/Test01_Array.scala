package chapter07

/**
 * @author yhm
 * @create 2021-09-16 16:21
 */
object Test01_Array {
  def main(args: Array[String]): Unit = {
    // 创建不可变数组
    val array = new Array[Int](10)
    // 也可以使用伴生对象的apply方法
    val array1: Array[Int] = Array(1, 2, 3, 4)

    // 遍历读取array
//    println(array)

    for (elem <- array1) {
      println(elem)
    }

    // 使用迭代器遍历数组
    val iterator: Iterator[Int] = array1.iterator

    while(iterator.hasNext){
      val i: Int = iterator.next()
      println(i)
    }

    println("===========================")

    // scala函数式编程的写法
    def myPrint(i:Int):Unit = {
      println(i)
    }

    // 放入自定义出来的函数
    array1.foreach(myPrint)
    // 直接使用匿名函数
    array1.foreach( i => println(i * 2) )
    // 最简单的打印形式 直接使用系统的函数
    array1.foreach(println)


    // 修改数组的元素
    println(array1(0))
    array1(0) = 10
    println(array1(0))

  }

}
