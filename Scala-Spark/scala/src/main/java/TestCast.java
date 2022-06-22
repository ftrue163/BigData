/**
 * @author yhm
 * @create 2021-09-11 15:30
 */
public class TestCast {
    public static void main(String[] args) {
        char n = 23;
        test(n);
    }

    public static void test(byte b) {
        System.out.println("bbbb");
    }

    public static void test(short b) {
        System.out.println("ssss");
    }

//    public static void test(char b) {
//        System.out.println("cccc");
//    }

    public static void test(int b) {
        System.out.println("iiii");
    }

}
