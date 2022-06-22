package chapter07

/**
 * @author yhm
 * @create 2021-09-17 9:57
 */
object Test03_ArrayDim {
  def main(args: Array[String]): Unit = {
    // 多维数组
    val arrayDim = new Array[Array[Int]](3)

    arrayDim(0) = Array(1,2,3,4)
    arrayDim(1) = Array(1,2,3,4)
    arrayDim(2) = Array(1,2,3,4)

    for (array <- arrayDim) {
      for (elem <- array) {
        print(elem + "\t")
      }
      println()
    }


    // scala中的方法
    val arrayDim1: Array[Array[Int]] = Array.ofDim[Int](3, 4)

    arrayDim1(0)(1) = 100
    for (array <- arrayDim1) {
      for (elem <- array) {
        print(elem + "\t")
      }
      println()
    }
  }
}
