package cn.edu.fudan.cs.db.performance.test.util;

import org.apache.http.client.fluent.Request;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by wujy on 16-1-17.
 */
public class FileUtil {

    public static void downloadURL(String url, String filename) throws IOException {
        System.out.print("Downloading `" + url + "` to `" + filename + "` ... ");
        File file = new File(filename);
        if (file.exists()) {
            System.out.println("Exists.");
            return;
        }
        Request.Get(url).execute().saveContent(new File(filename));
        System.out.println("Done.");

        Set<Integer> s = new HashSet<Integer>();
        s.addAll(s);
    }

}
