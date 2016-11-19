package cn.edu.fudan.cs.db.performance.test.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Created by wujy on 16-1-16.
 */
public class ByteUtil {

    public static byte[] toByteArray(List<String> stringList) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        for (int i = 0; i < stringList.size(); i++) {
            if (i > 0) out.writeUTF("_");
            out.writeUTF(stringList.get(i));
        }
        return baos.toByteArray();
    }

}
