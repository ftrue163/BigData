package chapter02

/**
 * @author yhm
 * @create 2021-09-11 15:33
 */
object Test06_TypeCast {
  def main(args: Array[String]): Unit = {
    //    （1）自动提升原则：有多种类型的数据混合运算时，
    //    系统首先自动将所有数据转换成精度大的那种数据类型，然后再进行计算。

    val fl: Float = 1 + 1L + 3.14f
    val d: Double = 1 + 1L + 3.14f + 3.14

    //    （2）把精度大的数值类型赋值给精度小的数值类型时，就会报错，反之就会进行自动类型转换。
    val i = 10
    val b: Double = i

    //    （3）（byte，short）和char之间不会相互自动转换。
    // 因为byte和short是有符号的数值,而char是无符号的
    val b1: Byte = 10
    val c1: Char = 20

    //    （4）byte，short，char他们三者可以计算，在计算时首先转换为int类型。
    val b2: Byte = 20
    //    val i1: Byte = b1 + b2
    val i1: Int = 1100000000
    val i2: Int = 1200000000
    // 超出范围的int值计算会造成结果错误
    val i3: Int = i1 + i2
    println(i3)


    // 强制类型转换
    val d1 = 2.999
    //    （1）将数据由高精度转换为低精度，就需要使用到强制转换
    println((d1 + 0.5).toInt)


    //    （2）强转符号只针对于最近的操作数有效，往往会使用小括号提升优先级
    println((10 * 3.5 + 6 * 1.5).toInt)

    //    （1）基本类型转String类型（语法：将基本类型的值+"" 即可）
    val string: String = 10.0.toString
    println(string)

    val str: String = 1 + ""

    //    （2）String类型转基本数值类型（语法：s1.toInt、s1.toFloat、s1.toDouble、s1.toByte、s1.toLong、s1.toShort）
    val double: Double = "3.14".toDouble

    println(double + 1)
    println(double.toInt)

    // 不能直接将小数类型的字符串转换为整数  需要先转换为double再转换int
    //    println("3.14".toInt)


    //    println("12.6f".toFloat)


    // 将int值130强转为byte  值为多少

    // 0000 0000 ..16.. 1000 0010   => 表示int的130
    val i4 = 130

    // 1000 0010
    println(i4.toByte)

  }
}
