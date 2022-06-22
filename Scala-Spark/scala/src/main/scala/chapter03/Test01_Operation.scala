package chapter03

/**
 * @author yhm
 * @create 2021-09-13 9:31
 */
object Test01_Operation {
  def main(args: Array[String]): Unit = {
    // 比较运算符
    // == 和equals的区别
    val str1 = "hello"
    val str2 = "hello"
    println(str1.equals(str2))
    println(str1 == str2)


    // 在scala当中 == 和equals都是在比较 值
    val str3 = new String("hello")
    val str4 = new String("hello")
    println(str3.equals(str4))
    println(str3 == str4)

    // eq比较地址值
    println(str3.eq(str4))


    // 逻辑运算符
    // 判断字符串非空

    def isNotEmpty(s: String): Boolean = {
      if (s == null) {
        return false
      }else {
        return !"".equals(s)
      }
    }

    def isNotEmpty1(s: String): Boolean = {
      !(s == null || s.equals(""))
      s != null && !s.trim().equals("")
    }

    println(isNotEmpty1("  1  "))


    1 + 1
    val i: Int = 1.+(10)


  }
}
