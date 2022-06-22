package chapter06

/**
 * @author yhm
 * @create 2021-09-16 11:03
 */
object Test11_Apply {
  def main(args: Array[String]): Unit = {
    //    val person1 = new Person11
    val person1: Person11 = Person11.getPerson11

    // 如果调用的方法是apply的话  方法名apply可以不写
    val person11: Person11 = Person11()

    val zhangsan: Person11 = Person11("zhangsan")

    // 类的apply方法调用
    person11()
  }
}


class Person11 private() {
  var name:String = _
  def this(name:String){
    this()
    this.name = name
  }

  def apply(): Unit = println("类的apply方法调用")
}


object Person11 {
  // 使用伴生对象的方法来获取对象实例
  def getPerson11: Person11 = new Person11

  // 伴生对象的apply方法
  def apply(): Person11 = new Person11()

  // apply方法的重载
  def apply(name: String): Person11 = new Person11(name)
}