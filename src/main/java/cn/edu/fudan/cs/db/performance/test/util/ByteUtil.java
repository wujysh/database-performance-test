package cn.edu.fudan.cs.db.performance.test.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Created by wujy on 16-1-16.
 */
public class ByteUtil {

    public static byte[] toByteArray(Integer i) {
        return new byte[]{i.byteValue()};
    }

    public static byte[] toByteArray(Float f) {
        return new byte[]{f.byteValue()};
    }

    public static byte[] toByteArray(String str) {
        return str.getBytes();
    }

    public static byte[] toByteArray(Boolean b) {
        return b.toString().getBytes();
    }

    public static byte[] toByteArray(Enum<?> e) {
        return e.name().getBytes();
    }

    public static byte[] toByteArray(List<String> stringList) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        for (String string : stringList) {
            out.writeUTF(string);
        }
        return baos.toByteArray();
    }

}
