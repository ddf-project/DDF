package utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by sangdn on 1/5/16.
 */
public class TestUtils {
    static String tabDelimiter = String.valueOf('\t');
    static String commaDelimiter = ",";

    /**
     * random generate user info
     * username age isMarried birthday
     *
     * @return
     */
    public static String generateUserInfo() {
        //birthday: dd MM yyyy
        String birthday = "%d %d %d";
        String date =String.format(birthday,genInt()%30,genInt()%12,1960 + genInt()%30);
        StringBuilder sb = new StringBuilder();
        sb.append(genString()).append(commaDelimiter)
                .append(genInt()).append(commaDelimiter)
                .append(genBool()).append(commaDelimiter)
                .append(date);
        return sb.toString();

    }


    public static int genInt() {
        return Math.abs(ThreadLocalRandom.current().nextInt() % 1000);
    }

    public static long genLong() {
        return Math.abs(ThreadLocalRandom.current().nextLong() % 1000);
    }

    public static List<Long> genListLong() {
        List<Long> list = new ArrayList<Long>();
        int n = genInt()%10 +1;
        for (int i = 0; i < n; ++i) {
            list.add((long) genInt());
        }
        return list;
    }

    public static List<String> genListString() {
        List<String> list = new ArrayList<String>();
        int n = genInt() % 10 +1;
        for (int i = 0; i < n; ++i) {
            list.add(genString());
        }
        return list;
    }

    public static String genString() {
        int n = genInt() % 10 + 1;
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < n; ++i) {
            sb.append((char) (65 + genInt() % 25));
        }
        return sb.toString();
    }

    public static boolean genBool() {
        return genInt() % 2 == 0;
    }
}
