package chapter08

/**
 * @author yhm
 * @create 2021-09-18 15:03
 */
object Test04_MatchList {
  def main(args: Array[String]): Unit = {
    // 匹配泛型
    // 如果匹配的是数组  能够匹配泛型
    def func1(x:AnyRef):String = {
      x match {
        case i:Array[Int] => "泛型为整数"
        case c:Array[Char] => "泛型为字符"
        case s:Array[String] => "泛型为字符串"
        case _ => "其他"
      }
    }

    println(func1(Array(1, 2, 3)))
    println(func1(Array('x', 'a')))
    println(func1(Array("hello")))
    println(func1(Array(3.14)))


    println("===========================")

    // 泛型擦除
    def func2(x:AnyRef):String = {
      x match {
        case c:List[Char] => "泛型为字符"
        case s:List[String] => "泛型为字符串"
        case i:List[Int] => "泛型为整数"
        case _ => "其他"
      }
    }

    println(func2(List(1,2,3)))
    println(func2(List('c')))
    println(func2(List("hello")))
    println(func2(List(1.1)))


    val list: List[Int] = List(1, 2, 5, 6, 7)

    list match {
      case first :: second :: rest => println(first + "-" + second + "-" + rest)
      case _ => println("something else")
    }


  }
}
