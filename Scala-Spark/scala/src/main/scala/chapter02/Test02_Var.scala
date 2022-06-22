package chapter02

/**
 * @author yhm
 * @create 2021-09-11 11:07
 */
object Test02_Var {
  def main(args: Array[String]): Unit = {
    // 声明变量和常量
    val a: Int = 10
    var b: Int = 20


    // 常量值无法修改
    //    a = 20
    b = 30
    //    （1）声明变量时，类型可以省略，编译器自动推导，即类型推导
    val c = 30


    //    （2）类型确定后，就不能修改，说明Scala是强数据类型语言。

    //    b = "30"

    //    （3）变量声明时，必须要有初始值
    val d: Int = 0

    //    var d1:Int = _

    val test02_Var = new Test02_Var()
    println(test02_Var.i)

    //    （4）var修饰的对象引用可以改变，val修饰的对象则不可改变，
    //    但对象的状态（值）却是可以改变的。（比如：自定义对象、数组、集合等等）
    val person0 = new Person02()
    var person1 = new Person02()

    // 引用数据类型的常量和变量能否替换成别的对象
    // var 可以修改引用数据类型的地址值  val不行
    person1 = new Person02()

    // 引用数据类型中的属性值能否发生变化  取决于内部的属性在定义的时候是var还是val
    //    person0.name = "lisi"
    person0.age = 11


  }
}

class Test02_Var {

  // scala中类的属性 如果是var变量也能使用默认值  但是必须要有等号
  var i: Int = _
}

class Person02 {
  val name: String = "zhangsan"
  var age: Int = 10
}
