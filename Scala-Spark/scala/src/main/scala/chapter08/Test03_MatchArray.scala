package chapter08

/**
 * @author yhm
 * @create 2021-09-18 14:35
 */
object Test03_MatchArray {
  def main(args: Array[String]): Unit = {
    val arrays: Array[Array[_ >: Int]] = Array(Array(0), Array(1, 0), Array(0, 1, 0), Array(1, 1, 0), Array(1, 1, 0, 1), Array("hello", 90))


    for (array <- arrays) {
      array match {
        case Array(0) => println("只有一个元素0的数组")
        case Array(1, _) => println("以1开头两个元素的数组")
        case Array(x, 1, y) => println(s"3个元素中间是1,左边是$x 右边是$y")
        case Array(x,y) => println(s"两个元素的数组  一个是 $x 一个是$y")
        case a:Array[Int] => println("整数数组")
      }
    }




  }
}
