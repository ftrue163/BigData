package chapter06

/**
 * @author yhm
 * @create 2021-09-16 15:16
 */
object Test17_Extends {
  def main(args: Array[String]): Unit = {
    // 多态表示
    val student1: Person17 = new Student17
    val person1 = new Person17

    def sayHi(p:Person17)={
      println(s"hi ${p.name}")

      // 如果传入的不是student  强制转换会报错 需要先判断
      if (p.isInstanceOf[Student17]){
        // 转换为student类 调用子类的方法
        val student11: Student17 = p.asInstanceOf[Student17]
        student11.sayHello
      }
    }


    sayHi(student1)
    sayHi(person1)

    // 获取类模板
    // 和java的反射完全一样  在scala当中不太需要用到
    val value: Class[Student17] = classOf[Student17]


    // 枚举类的调用
    val red: Color.Value = Color.RED
    val string: String = red.toString
    println(string)


    // 定义新类型
    type s = String

    val s1:s = "hello"

    val world: s = "world"

  }
}

class Person17{
  val name = "zhangsan"
}

class Student17 extends Person17{
  def sayHello: Unit ={
    println("hello student")
  }
}


// 枚举类
object Color extends Enumeration{
  val RED: Color.Value = Value(1,"red")
}

// 应用类
object app extends App{
  println("hello world")
}