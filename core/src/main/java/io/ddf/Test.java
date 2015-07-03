package io.ddf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jing on 7/1/15.
 */
public class Test {
    public static void main(String[] args) {
        Pattern pattern = Pattern.compile("ddf:\\/\\/.*");
        Matcher m = pattern.matcher("ddf://adatao/a");
        if (m.matches()) {
            System.out.println("success");
        } else {
            System.out.println("fail");
        }
    }
}
