package chapter06Test

import chapter06.Person04

/**
 * @author yhm
 * @create 2021-09-15 16:11
 */
object Test04_Access {
  def main(args: Array[String]): Unit = {
    // 不同的包里面无法访问name1  包访问权限
//    Person04.name1

    // 不同的包也能访问到公共的权限
    Person04.name3
  }
}

class Student04 extends Person04{
  // 即使不是一个包  继承的子类也能够访问到受保护的权限
  val name2Class = name2
}
