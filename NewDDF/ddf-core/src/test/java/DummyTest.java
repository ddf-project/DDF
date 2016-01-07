import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;

import java.sql.Timestamp;

/**
 * Created by sangdn on 12/30/15.
 */
public class DummyTest {
    public static void main(String[] args) {
//        testTernaryOperator();
//        testTypeParser();
        listHiveDataType();

    }

    public static final void testTernaryOperator(){
        int i =-1;
        int temp = 10;
        int j = i >=0 ? i : (i = temp);
        System.out.println("j=" + j + " i= " + i);
    }

    public static void testTypeParser(){
        String s = "1.1";
        try {
            long l = Long.parseLong(s);
        }catch(NumberFormatException ex){
            System.out.println("Expected NFE To Parse Double To Long");
        }
        double d = Double.parseDouble(s);
        assert d == 1.1;
        out(s,"1.1",String.valueOf(d));
        
        s = "false";
        boolean b = Boolean.parseBoolean(s);
        out(s,"false",String.valueOf(b));
        s = "true";
        b = Boolean.parseBoolean(s);
        out(s, "true", String.valueOf(b));


        s = "1";
        d = Double.parseDouble(s);
        out(s, "1", String.valueOf(d));
    }
    public static void out(String input, String expect,String actual){
        System.out.println("Parse " + input + " Expect=" + expect + "Result=" + actual);
    }

    public static void listHiveDataType(){
        System.out.println(DataType.fromCaseClassString("DecimalType"));
    }
}
