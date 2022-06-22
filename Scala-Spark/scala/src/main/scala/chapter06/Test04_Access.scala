package chapter06

/**
 * @author yhm
 * @create 2021-09-15 16:07
 */
object Test04_Access {
  def main(args: Array[String]): Unit = {

    // 同一个包都可以访问的到
    Person04.name1

    // 受保护的权限  同一个包也无法访问
//    Person04.name2

    // 访问公共的权限
    Person04.name3
  }
}


class Person04{
  val nameClass = Person04.name

  val name1Class = Person04.name1

  // 受保护的权限
  protected val name2:String = "受保护的权限"
}

object Person04{
  // 私有的权限能够在当前类和当前伴生对象中调用
  private val name:String = "私有权限"

  // 包访问权限
  private[chapter06] val name1:String = "包访问权限"

  // public的权限
  val name3:String = "公共的权限"


}
