/**
 * @author yhm
 * @create 2021-09-13 9:31
 */
public class TestEquals {
    public static void main(String[] args) {
        String str1 = "hello";
        String str2 = "hello";

        // equals默认都有重写  重写之后比较的是值
        // == 没有任何重写 比较的是地址值

        // 字符串是特殊的对象  放到常量池中的
        System.out.println(str1.equals(str2));
        System.out.println(str1 == str2);


        // 如果手动去创建对象  则返回的不在常量池中
        String str3 = new String("hello");
        String str4 = new String("hello");

        System.out.println(str3.equals(str4));
        System.out.println(str3 == str4);

    }
}
