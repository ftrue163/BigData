package chapter09add

/**
 * @author yhm
 * @create 2021-09-18 16:24
 */
object Test03_Genericity {
  def main(args: Array[String]): Unit = {
    // 协变和逆变
    var father: MyList[Father] = new MyList[Father]
    var son: MyList[Son] = new MyList[Son]

    // 多态
    // T表示不变  没有父子关系
    // +T 表示协变
    // -T 表示逆变
    //    father = son
    //    son = father

  }


  class MyList[-T] {

  }

  class Father {

  }

  class Son extends Father {

  }

}
