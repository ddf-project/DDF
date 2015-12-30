/**
 * Created by sangdn on 12/30/15.
 */
public class DummyTest {
    public static void main(String[] args) {
        testTernaryOperator();
    }
    public static final void testTernaryOperator(){
        int i =-1;
        int temp = 10;
        int j = i >=0 ? i : (i = temp);
        System.out.println("j=" + j + " i= " +i);
    }
}
