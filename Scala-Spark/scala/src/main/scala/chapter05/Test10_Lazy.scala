package chapter05

/**
 * @author yhm
 * @create 2021-09-15 14:35
 */
object Test10_Lazy {
  def main(args: Array[String]): Unit = {
    def sumAB(a:Int,b:Int):Int = {
      println("sumAB调用")
      a + b
    }


    lazy val n = sumAB(10,20)

    println("分隔符===================")

    println(n)


  }
}
