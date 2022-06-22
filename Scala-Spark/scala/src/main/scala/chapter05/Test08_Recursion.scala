package chapter05

import scala.annotation.tailrec

/**
 * @author yhm
 * @create 2021-09-15 11:23
 */
object Test08_Recursion {
  def main(args: Array[String]): Unit = {

    // 阶乘
    var n = 5
    var res = 1
    for (i <- 1 to n) {
      res *= i
    }

    println(res)

    // 递归
    // 1. 调用自身
    // 2. 跳出条件
    // 3. 填入的参数必须有规律

    def rec(n: Int): Int = {
      if (n == 1) 1 else rec(n - 1) * n
    }

    println(rec(5))


    // 尾递归优化
    @tailrec
    def rec1(n: Int, res: Int = 1): Int = {

      rec1(n - 1,res * n)
    }

    println(rec1(5))

  }
}
