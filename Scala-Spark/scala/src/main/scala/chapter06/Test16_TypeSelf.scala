package chapter06

/**
 * @author yhm
 * @create 2021-09-16 15:11
 */
object Test16_TypeSelf {
  def main(args: Array[String]): Unit = {
    //    val young1: Young16 = new Young16 {
    //      override val age: Int = 16
    //    }
  }
}


trait Age16 {
  val age: Int
}

//trait Young16  extends Age16


trait Young16 {
  // 特质自身类型
  _: Age16 =>
}

// 特质自身类型 要求必须同时实现两个依赖的特质
class Person16 extends Young16 with Age16{
  override val age: Int = 17
}