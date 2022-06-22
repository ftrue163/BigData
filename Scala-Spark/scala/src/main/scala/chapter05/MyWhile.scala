package chapter05

/**
 * @author yhm
 * @create 2021-09-15 14:27
 */
object MyWhile {
  def main(args: Array[String]): Unit = {
    // 名调用自定义出  while循环
    var n = 0
    while (n < 5){
      println(n)
      n += 1
    }

    //
    def myWhile(b: => Boolean)( op: => Unit ): Unit ={
      if (b) {
        op
        myWhile(b)(op)
      }
    }

    n = 0
    myWhile(n < 5)({
      println(n)
      n += 1
    })

  }
}
