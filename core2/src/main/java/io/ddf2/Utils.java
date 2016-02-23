package io.ddf2;

import java.math.BigDecimal;
import java.text.DecimalFormat;

/**
 * Created by sangdn on 1/26/16.
 */
public class Utils {
    public static double formatDouble(double number) {
        DecimalFormat fmt = new DecimalFormat("#.##");
        if (Double.isInfinite(number) || Double.isNaN(number)) {
            return Double.NaN;
        } else {
            return Double.parseDouble((fmt.format(number)));
        }
    }

    public static double round(double number, int precision, int mode) {
        if (Double.isInfinite(number) || Double.isNaN(number)) {
            return Double.NaN;
        } else {
            BigDecimal bd = new BigDecimal(number);
            return bd.setScale(precision, mode).doubleValue();
        }
    }

    public static double roundUp(double number) {
        if (Double.isInfinite(number) || Double.isNaN(number)) {
            return Double.NaN;
        } else {
            return round(number, 2, BigDecimal.ROUND_HALF_UP);
        }
    }
}
