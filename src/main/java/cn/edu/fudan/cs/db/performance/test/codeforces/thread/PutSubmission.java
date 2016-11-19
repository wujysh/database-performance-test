package cn.edu.fudan.cs.db.performance.test.codeforces.thread;

import cn.edu.fudan.cs.db.performance.test.codeforces.DataProvider;
import cn.edu.fudan.cs.db.performance.test.codeforces.entity.Submission;
import cn.edu.fudan.cs.db.performance.test.util.StringUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wujy on 16-1-18.
 */
public class PutSubmission implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(PutSubmission.class.getName());

    private Table table = null;

    private List<Put> pendingPutCache = new ArrayList<>();

    public PutSubmission(Connection conn) {
        try {
            table = conn.getTable(TableName.valueOf("codeforces:submission"));
            pendingPutCache.clear();
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
        }
    }

    public void run() {
        System.out.println("Submission put thread started.");

        while (true) {
            try {
                Submission submission = DataProvider.unPutSubmissionQueue.take();
                if (pendingPutCache.size() > 100000 || null == submission) {
                    table.put(pendingPutCache);
                    pendingPutCache.clear();
                }
                if (null == submission) {
                    System.out.println("Done.");
                    Thread.sleep(100);
                    continue;
                }

//                System.out.print("Putting to HBase: " + submission.getId() + ": " + submission.getProblem().getContestId() + " " + submission.getProblem().getIndex() + " ... ");

                /*
                 * Table            codeforces:submission
                 *
                 * Row key          {problem.contestId}(padding)-{relativeTimeSeconds}(padding)-{problem.index}-{author.members.handle}-{id}
                 *
                 * Column Family 1  verdict
                 *                  verdict
                 *
                 * Column Family 2  info
                 * Columns          creationTimeSeconds,
                 *                  participantType, teamId, teamName, ghost, room, startTimeSeconds,
                 *                  programmingLanguage, testset, passedTestCount,
                 *                  timeConsumedMillis, memoryConsumedBytes
                 *
                 * Column Family 3  code
                 * Columns          code
                 */
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(submission.getProblem().getContestIdWithPaddingZero()).append("-")
                        .append(StringUtil.numWithPadding(submission.getRelativeTimeSeconds(), 10)).append("-")
                        .append(submission.getProblem().getIndex()).append("-");
                if (submission.getAuthor().getMembers().isEmpty()) {
                    stringBuilder.append(submission.getAuthor().getTeamName());
                } else {
                    for (int i = 0; i < submission.getAuthor().getMembers().size(); i++) {
                        if (i > 0) stringBuilder.append("_");
                        stringBuilder.append(submission.getAuthor().getMembers().get(i).getHandle());
                    }
                }
                stringBuilder.append("-").append(submission.getId());

                Put put = new Put(Bytes.toBytes(stringBuilder.toString()));

                if (submission.getVerdict() != null)
                    put.addColumn(Bytes.toBytes("verdict"), Bytes.toBytes("verdict"), Bytes.toBytes(String.valueOf(submission.getVerdict())));

                if (submission.getCreationTimeSeconds() != null)
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("creationTimeSeconds"), Bytes.toBytes(submission.getCreationTimeSeconds()));
                if (submission.getAuthor().getParticipantType() != null)
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("participantType"), Bytes.toBytes(String.valueOf(submission.getAuthor().getParticipantType())));
                if (submission.getAuthor().getTeamId() != null)
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("teamId"), Bytes.toBytes(submission.getAuthor().getTeamId()));
                if (submission.getAuthor().getTeamName() != null)
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("teamName"), Bytes.toBytes(submission.getAuthor().getTeamName()));
                if (submission.getAuthor().getGhost() != null)
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ghost"), Bytes.toBytes(submission.getAuthor().getGhost()));
                if (submission.getAuthor().getRoom() != null)
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("room"), Bytes.toBytes(submission.getAuthor().getRoom()));
                if (submission.getAuthor().getStartTimeSeconds() != null)
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("startTimeSeconds"), Bytes.toBytes(submission.getAuthor().getStartTimeSeconds()));
                if (submission.getProgrammingLanguage() != null)
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("programmingLanguage"), Bytes.toBytes(submission.getProgrammingLanguage()));
                if (submission.getTestset() != null)
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("testset"), Bytes.toBytes(String.valueOf(submission.getTestset())));
                if (submission.getPassedTestCount() != null)
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("passedTestCount"), Bytes.toBytes(submission.getPassedTestCount()));
                if (submission.getTimeConsumedMillis() != null)
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("timeConsumedMillis"), Bytes.toBytes(submission.getTimeConsumedMillis()));
                if (submission.getMemoryConsumedBytes() != null)
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("memoryConsumedBytes"), Bytes.toBytes(submission.getMemoryConsumedBytes()));

                pendingPutCache.add(put);
            } catch (InterruptedException | IOException e) {
                logger.error(e.getMessage(), e.getCause());
            }
        }
    }

}