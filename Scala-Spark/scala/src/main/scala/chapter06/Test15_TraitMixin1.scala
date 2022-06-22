package chapter06

/**
 * @author yhm
 * @create 2021-09-16 15:01
 */
object Test15_TraitMixin1 {
  def main(args: Array[String]): Unit = {
    val person1 = new Person15

    println(person1.info)
  }
}


trait Age15{
  def info: String ={
    " age"
  }
}

trait Young15 extends Age15{
  override def info: String = " young " + super.info
}

trait Old15 extends Age15{
  override def info: String = " old " + super.info
}

// 特质叠加
class Person15 extends Young15 with  Old15{
//  override def info: String = "person " + super[Old15].info
  override def info: String = "person " + super.info
}
