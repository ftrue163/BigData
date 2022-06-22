package chapter04

/**
 * @author yhm
 * @create 2021-09-13 11:36
 */
object Problems {
  def main(args: Array[String]): Unit = {
    // 需求一: 打印乘法口诀表 9*9
    for (i <- 1 to 9; j <- 1 to i) {
      print(s"$j * $i = ${j * i}\t")
      if (j == i) println()
    }


    // 需求二: 打印9层妖塔
    //  *
    // ***
    //*****

    for (i <- 1 to 9) {
      println(" " * (9 - i) + "*" * (2 * i - 1))
    }


    for {
      i <- 1 to 9
      j = 9 - i
      k = 2 * i - 1
    }{
      println(" " * j + "*" * k)
    }

    for {
      j <- 8 to 0 by -1
      k = 17 - 2 * j
    }{
      println(" " * j + "*" * k)
    }


  }
}
