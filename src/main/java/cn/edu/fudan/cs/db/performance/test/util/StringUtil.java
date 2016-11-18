package cn.edu.fudan.cs.db.performance.test.util;

/**
 * Created by wujy on 16-11-17.
 */
public class StringUtil {

    public static String numWithPadding(int num, int numOfPadding, String padding) {
        String strNum = String.valueOf(num);
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < numOfPadding - strNum.length(); i++) {
            stringBuilder.append(padding);
        }
        stringBuilder.append(strNum);
        return stringBuilder.toString();
    }

    public static String numWithPadding(int num, int numOfPadding) {
        return numWithPadding(num, numOfPadding, "0");
    }

}
