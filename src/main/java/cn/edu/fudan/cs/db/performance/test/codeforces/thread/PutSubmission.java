package cn.edu.fudan.cs.db.performance.test.codeforces.thread;

import cn.edu.fudan.cs.db.performance.test.codeforces.DataProvider;
import cn.edu.fudan.cs.db.performance.test.codeforces.entity.Submission;
import cn.edu.fudan.cs.db.performance.test.util.ByteUtil;
import cn.edu.fudan.cs.db.performance.test.util.StringUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wujy on 16-1-18.
 */
public class PutSubmission implements Runnable {

    private Table table = null;
    private List<Put> unPutList = new ArrayList<>();

    public PutSubmission(Connection conn) {
        try {
            table = conn.getTable(TableName.valueOf("codeforces:submission"));
            unPutList.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        System.out.println("Submission put thread started.");

        while (true) {
            try {
                Submission submission = DataProvider.unPutSubmissionQueue.take();
                if (unPutList.size() > 100000 || null == submission) {
                    table.put(unPutList);
                    unPutList.clear();
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
                 * Row key          {problem.contestId}(padding)-{relativeTimeSeconds}(padding)-{problem.index}-{author.members.handle}
                 *
                 * Column Family 1  verdict
                 *                  verdict
                 *
                 * Column Family 2  info
                 * Columns          id, creationTimeSeconds,
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

                Put put = new Put(stringBuilder.toString().getBytes());

                if (submission.getVerdict() != null)
                    put.addColumn("verdict".getBytes(), "verdict".getBytes(), ByteUtil.toByteArray(submission.getVerdict()));

                if (submission.getId() != null)
                    put.addColumn("info".getBytes(), "id".getBytes(), ByteUtil.toByteArray(submission.getId()));
                if (submission.getCreationTimeSeconds() != null)
                    put.addColumn("info".getBytes(), "creationTimeSeconds".getBytes(), ByteUtil.toByteArray(submission.getCreationTimeSeconds()));
                if (submission.getAuthor().getParticipantType() != null)
                    put.addColumn("info".getBytes(), "participantType".getBytes(), ByteUtil.toByteArray(submission.getAuthor().getParticipantType()));
                if (submission.getAuthor().getTeamId() != null)
                    put.addColumn("info".getBytes(), "teamId".getBytes(), ByteUtil.toByteArray(submission.getAuthor().getTeamId()));
                if (submission.getAuthor().getTeamName() != null)
                    put.addColumn("info".getBytes(), "teamName".getBytes(), ByteUtil.toByteArray(submission.getAuthor().getTeamName()));
                if (submission.getAuthor().getGhost() != null)
                    put.addColumn("info".getBytes(), "ghost".getBytes(), ByteUtil.toByteArray(submission.getAuthor().getGhost()));
                if (submission.getAuthor().getRoom() != null)
                    put.addColumn("info".getBytes(), "room".getBytes(), ByteUtil.toByteArray(submission.getAuthor().getRoom()));
                if (submission.getAuthor().getStartTimeSeconds() != null)
                    put.addColumn("info".getBytes(), "startTimeSeconds".getBytes(), ByteUtil.toByteArray(submission.getAuthor().getStartTimeSeconds()));
                if (submission.getProgrammingLanguage() != null)
                    put.addColumn("info".getBytes(), "programmingLanguage".getBytes(), ByteUtil.toByteArray(submission.getProgrammingLanguage()));
                if (submission.getTestset() != null)
                    put.addColumn("info".getBytes(), "testset".getBytes(), ByteUtil.toByteArray(submission.getTestset()));
                if (submission.getPassedTestCount() != null)
                    put.addColumn("info".getBytes(), "passedTestCount".getBytes(), ByteUtil.toByteArray(submission.getPassedTestCount()));
                if (submission.getTimeConsumedMillis() != null)
                    put.addColumn("info".getBytes(), "timeConsumedMillis".getBytes(), ByteUtil.toByteArray(submission.getTimeConsumedMillis()));
                if (submission.getMemoryConsumedBytes() != null)
                    put.addColumn("info".getBytes(), "memoryConsumedBytes".getBytes(), ByteUtil.toByteArray(submission.getMemoryConsumedBytes()));

                unPutList.add(put);
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }
    }

}