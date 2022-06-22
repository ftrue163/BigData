package chapter02

/**
 * @author yhm
 * @create 2021-09-11 14:00
 */
object Test03_Str {
  def main(args: Array[String]): Unit = {

    //    （1）字符串，通过+号连接
    System.out.println()
    println("hello" + "world")

    //    （2）重复字符串拼接
    println("linhailinhai" * 200)

    //    （3）printf用法：字符串，通过%传值。
    printf("name: %s age: %d\n", "linhai", 8)

    //    （4）字符串模板（插值字符串）：通过$获取变量值
    val name = "linhai"
    val age = 8

    val s1 = s"name: $name,age:${age}"
    println(s1)

    val s2 = s"name: ${name + 1},age:${age + 2}"
    println(s2)


    //    （5）长字符串  原始字符串
    println("我" +
      "是" +
      "一首" +
      "诗")
    //多行字符串，在Scala中，利用三个双引号包围多行字符串就可以实现。
    // 输入的内容，带有空格、\t之类，导致每一行的开始位置不能整洁对齐。
    //应用scala的stripMargin方法，在scala中stripMargin默认是“|”作为连接符，
    // 在多行换行的行头前面加一个“|”符号即可。

    println(
      """我
        |是
        |一首
        |诗
        |""".stripMargin)

    """
      |select id,
      |       age
      |from  user_info
      |""".stripMargin


    s"""
       |${name}
       |${age}
       |""".stripMargin

  }
}
