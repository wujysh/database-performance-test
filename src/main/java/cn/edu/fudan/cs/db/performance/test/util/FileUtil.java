package cn.edu.fudan.cs.db.performance.test.util;

import org.apache.http.client.fluent.Request;

import java.io.File;
import java.io.IOException;

/**
 * Created by wujy on 16-1-17.
 */
public class FileUtil {

    public static void downloadURL(String url, String filename) throws IOException {
        downloadURL(url, filename, false);
    }

    public static void downloadURL(String url, String filename, boolean override) throws IOException {
        System.out.print("Downloading `" + url + "` to `" + filename + "` ... ");
        File file = new File(filename);
        if (file.exists()) {
            if (!override) {
                System.out.println("Exists.");
                return;
            }
            file.delete();
        }
        Request.Get(url).execute().saveContent(new File(filename));
        System.out.println("Downloaded.");
    }

}
