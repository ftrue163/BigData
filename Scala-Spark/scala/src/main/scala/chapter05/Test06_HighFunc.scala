package chapter05

/**
 * @author yhm
 * @create 2021-09-15 9:20
 */
object Test06_HighFunc {
  def main(args: Array[String]): Unit = {
    def sayHi(name: String): String = {
      println(s"hi $name")
      s"hi $name"
    }

    sayHi("linhai")

    //    1）函数可以作为值进行传递
    val func1: String = sayHi("linhai")
    val func2 = sayHi _
    val func3: String => String = sayHi

    func2("jinlian")
    func3("dalang")

    //    2）函数可以作为参数进行传递
    def sumAB(a: Int, b: Int): Int = a + b

    def difAB(a: Int, b: Int): Int = a - b

    // 给两个数 ,之后按照传入的公式进行计算
    def funcAB(a: Int, b: Int, func: (Int, Int) => Int): Int = {
      func(a, b)
    }

    //    def mr(data:String,map:String => String,reduce:String => String) :String ={
    //      val str: String = map(data)
    //      reduce(str)
    //    }

    val i: Int = funcAB(10, 20, difAB)
    println(i)


    funcAB(10, 20, 2 * _ / 4 *_)

    //    3）函数可以作为函数返回值返回
    def sumByX(x:Int) = {
      def sumXY(y: Int): Int = {
        x + y
      }

      sumXY _
    }

    val function: Int => Int = sumByX(10)
    println(function)
    val i1: Int = function(20)
    println(i1)

    val i2: Int = sumByX(10)(20)
    println(i2)

  }
}
