package chapter08

/**
 * @author yhm
 * @create 2021-09-18 15:25
 */
object Test06_PartitionFunc {
  def main(args: Array[String]): Unit = {


    val pFunc: PartialFunction[List[Int], Option[Int]] = new PartialFunction[List[Int], Option[Int]] {
      override def isDefinedAt(x: List[Int]): Boolean = {
        x match {
          case x :: y :: list => true
          case _ => false
        }
      }

      override def apply(v1: List[Int]): Option[Int] = {
        v1 match {
          case x :: y :: list => Some(y)
        }
      }
    }


    // 将该List(1,2,3,4,5,6,"test")中的Int类型的元素加一，并去掉字符串。

    val list = List(1, 2, 3, 4, 5, 6, "test")

    // 步骤一: 过滤掉字符串
    val list1: List[Any] = list.filter((a: Any) => a match {
      case s: String => false
      case i: Int => true
    })

    // 步骤二: 对int值加一
    val list2: List[Int] = list1.map((a: Any) => {
      a match {
        case i: Int => i + 1
      }
    })

    println(list2)

    val list3: List[Int] = list.collect({
      case i: Int => i + 1
    })

    println(list3)

    val value:PartialFunction[Any, Int] =  {
      case i: Int => i + 1
    }

    // 函数的定义 需要多写一个math关键字
    // 偏函数将match关键字省略
    val function: Any => Int = (a: Any) => a match {
      case i: Int => i + 1
    }


  }
}
