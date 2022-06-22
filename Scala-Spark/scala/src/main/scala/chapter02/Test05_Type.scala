package chapter02

/**
 * @author yhm
 * @create 2021-09-11 14:31
 */
object Test05_Type {
  def main(args: Array[String]): Unit = {

    // 任何代码都会被当做代码块执行  最终将最后一行代码的返回值 返回
    val i: Int = {
      9
      2 + 1
      10
    }

    val unit: Unit = println(i)
    //        val value: Nothing = throw new RuntimeException()


    // 整数类型
    val i1 = 1

    val l = 1L

    // （1）Scala各整数类型有固定的表示范围和字段长度，不受具体操作的影响，以保证Scala程序的可移植性。
    val b1: Byte = 2
    //    val b0: Byte = 128

    val b2: Byte = 1 + 1
    println(b2)

    val i2 = 1

    // 编译器对于常量值的计算 能够直接使用结果进行编译
    // 但是如果是变量值 编译器是不知道变量的值的 所以判断不能将大类型的值赋值给小的类型
    //    val b3: Byte = i2 + 1
    //    println(b3)

    // （3）Scala程序中变量常声明为Int型，除非不足以表示大数，才使用Long
    val l1 = 2200000000L


    // 浮点数介绍
    // 默认使用double
    val d: Double = 3.14
    // 如果使用float 在末尾添加f
    val fl = 3.14f
    // 浮点数计算有误差
    println(0.1 / 3.3)


    // 字符类型
    //    （1）字符常量是用单引号 ' ' 括起来的单个字符。
    val c1: Char = 'a'
    val c2: Char = 65535

    //    （2）\t ：一个制表位，实现对齐的功能
    val c3: Char = '\t'

    //    （3）\n ：换行符
    val c4: Char = '\n'
    println(c3 + 0)
    println(c4 + 0)

    //    （4）\\ ：表示\
    val c5: Char = '\\'
    println(c5 + 0)

    //    （5）\" ：表示"
    val c6: Char = '\"'
    println(c6 + 0)

    // boolean 类型
    val bo1: Boolean = true
    val bo2: Boolean = false


    // unit
    val unit1: Unit = {
      10
      println("1")
    }
    println(unit1)

    // 如果标记对象的类型是unit的话  后面有返回值也没法接收
    // unit虽然是数值类型  但是可以接收引用数据类型   因为都是表示不接收返回值
    val i3: Unit = "aa"
    println(i3)

    // scala当中使用的字符串就是java中的string
    val aa: String = "aa"

    val value: Nothing = {
      println("hello")
      1 + 1
      throw new RuntimeException()
    }

    // null
    var aa1: String = "aa"

    aa1 = "bb"
    aa1 = null
    if (aa1 != null) {
      val strings: Array[String] = aa1.split(",")
    }

    // 值类型不能等于null,idea不会识别报错  编译器会报错
    var i4 = 10
    //    i4 = null


  }
}
