package cn.edu.fudan.cs.db.performance.test.util;

import org.apache.http.client.fluent.Request;

import java.io.File;
import java.io.IOException;

/**
 * Created by wujy on 16-1-17.
 */
public class FileUtil {

    public static void downloadURL(String url, String filename) throws IOException {
        System.out.print("Downloading `" + url + "` to `" + filename + "` ... ");
        Request.Get(url).execute().saveContent(new File(filename));
        System.out.println("Done.");
    }

}
