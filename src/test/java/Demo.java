import org.junit.Test;

public class Demo {

    @Test
    public void test() {


        String [] a  = {"a","b","c","d"};

        for (String s : a) {

            for (String s1 : a) {

                System.out.println(s+","+s1);
            }
        }

    }
}
