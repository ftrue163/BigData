package chapter07

/**
 * @author yhm
 * @create 2021-09-18 14:14
 */
object Test17_ParList {
  def main(args: Array[String]): Unit = {
    //    (1 to 100).foreach((i: Int) => println(i + "-" + Thread.currentThread().getName))


    // 并行集合演示
    (1 to 100).par.foreach((i: Int) => println(i + "-" + Thread.currentThread().getName))

  }
}
