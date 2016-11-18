package cn.edu.fudan.cs.db.performance.test.codeforces;

import cn.edu.fudan.cs.db.performance.test.codeforces.entity.Submission;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by wujy on 16-1-18.
 */
public class DataProvider {

    public static BlockingQueue<Submission> unPutSubmissionQueue = new LinkedBlockingDeque<>();

}
