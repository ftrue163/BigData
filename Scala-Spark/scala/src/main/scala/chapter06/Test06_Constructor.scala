package chapter06

/**
 * @author yhm
 * @create 2021-09-16 9:25
 */
object Test06_Constructor {
  def main(args: Array[String]): Unit = {
    val person0 = new Person06("zhangsan")
    val person01 = new Person06()

    println(person01.name1)

    val person02 = new Person06("lisi", 18)
  }
}


// 主构造器  直接写在类的定义后面  可以添加参数  可以使用权限修饰符
//class Person06 private(name:String){
//  val name1 = name
//}

class Person06 (name:String){
  println("调用主构造器")

  val name1 = name
  var age:Int = _

  // 两个辅助构造器  再互相调用的时候 只能是下面的辅助构造器调用上面的辅助构造器

  def this(){
    // 辅助构造器的第一行 必须直接或简介的调用主构造器
    // 直接调用主构造器
    this("zhangsan")
    println("调用辅助构造器1")
  }

  def this(name:String,age1:Int){
    // 间接调用主构造器
    this()
    this.age = age1
    println("调用辅助构造器2")
  }


}
