package cn.edu.fudan.cs.db.performance.test.codeforces;

import cn.edu.fudan.cs.db.performance.test.codeforces.entity.Contest;
import cn.edu.fudan.cs.db.performance.test.codeforces.entity.Problem;
import cn.edu.fudan.cs.db.performance.test.codeforces.entity.ProblemStatistics;
import cn.edu.fudan.cs.db.performance.test.codeforces.entity.User;
import cn.edu.fudan.cs.db.performance.test.codeforces.response.*;
import cn.edu.fudan.cs.db.performance.test.codeforces.thread.PutSubmission;
import cn.edu.fudan.cs.db.performance.test.util.ByteUtil;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.*;
import java.net.MalformedURLException;
import java.util.List;

/**
 * Created by wujy on 16-1-16.
 */
public class Main {

    private static Connection conn = null;

    private static Gson gson = new Gson();

    private static String BASE_DATA_DIR = "data" + File.separator;

    public static void initHBase() throws IOException {
        Admin admin = null;
        try {
            conn = ConnectionFactory.createConnection(HBaseConfiguration.create());
            admin = conn.getAdmin();

            System.out.println("Checking HBase Namespace `codeforces`");
            try {
                admin.listTableNamesByNamespace("codeforces");
            } catch (NamespaceNotFoundException e) {
                NamespaceDescriptor namespace = NamespaceDescriptor.create("codeforces").build();
                admin.createNamespace(namespace);
            }

            /**
             * Table            codeforces:user
             *
             * Row key          {handle}
             * Column Family 1  info
             * Columns          email, vkId, openId, firstName, lastName, country, city, organization,
             *                  contribution, lastOnlineTimeSeconds, registrationTimeSeconds
             *
             * Column Family 2  rank
             * Columns          rank, rating, maxRank, maxRating
             */
            System.out.println("Checking HBase table `codeforces:user`");
            TableName tableNameUser = TableName.valueOf("codeforces:user");
            if (!admin.tableExists(tableNameUser)) {
                System.out.println("Creating HBase table `codeforces:user`");
                HTableDescriptor htd = new HTableDescriptor(tableNameUser);
                HColumnDescriptor hcdInfo = new HColumnDescriptor("info");
                HColumnDescriptor hcdRank = new HColumnDescriptor("rank");
                htd.addFamily(hcdInfo);
                htd.addFamily(hcdRank);
                admin.createTable(htd);
            }

            /**
             * Table            codeforces:contest
             *
             * Row key          {id}
             * Column Family 1  info
             * Columns          name, type, phase, frozen, preparedBy, websiteUrl, description,
             *                  difficulty, kind, icpcRegion, city, season
             *
             * Column Family 2  time
             * Columns          startTimeSeconds, durationSeconds, relativeTimeSeconds
             */
            System.out.println("Checking HBase table `codeforces:contest`");
            TableName tableNameContest = TableName.valueOf("codeforces:contest");
            if (!admin.tableExists(tableNameContest)) {
                System.out.println("Creating HBase table `codeforces:contest`");
                HTableDescriptor htd = new HTableDescriptor(tableNameContest);
                HColumnDescriptor hcdInfo = new HColumnDescriptor("info");
                HColumnDescriptor hcdTime = new HColumnDescriptor("time");
                htd.addFamily(hcdInfo);
                htd.addFamily(hcdTime);
                admin.createTable(htd);
            }

            /**
             * Table            codeforces:problem
             *
             * Row key          {contestId}-{index}
             * Column Family 1  info
             * Columns          name, type, points, tags, solvedCount
             *
             * Column Family 2  html
             * Columns          content, timeLimit, memoryLimit, ...
             */
            System.out.println("Checking HBase table `codeforces:problem`");
            TableName tableNameProblem = TableName.valueOf("codeforces:problem");
            if (!admin.tableExists(tableNameProblem)) {
                System.out.println("Creating HBase table `codeforces:problem`");
                HTableDescriptor htd = new HTableDescriptor(tableNameProblem);
                HColumnDescriptor hcdInfo = new HColumnDescriptor("info");
                HColumnDescriptor hcdHtml = new HColumnDescriptor("html");
                htd.addFamily(hcdInfo);
                htd.addFamily(hcdHtml);
                admin.createTable(htd);
            }

            /**
             * Table            codeforces:submission
             *
             * Row key          {problem.contestId}-{relativeTimeSeconds}-{author.members.handle}
             * Column Family 1  info
             * Columns          programmingLanguage, verdict, testset, passedTestCount,
             *                  timeConsumedMillis, memoryConsumedBytes,
             *                  participantType, teamId, teamName, ghost, room, startTimeSeconds,
             *                  id, problem.index
             *
             * Column Family 2  code
             * Columns          code
             */
            System.out.println("Checking HBase table `codeforces:submission`");
            TableName tableNameSubmission = TableName.valueOf("codeforces:submission");
            if (!admin.tableExists(tableNameSubmission)) {
                System.out.println("Creating HBase table `codeforces:submission`");
                HTableDescriptor htd = new HTableDescriptor(tableNameSubmission);
                HColumnDescriptor hcdInfo = new HColumnDescriptor("info");
                HColumnDescriptor hcdCode = new HColumnDescriptor("code");
                htd.addFamily(hcdInfo);
                htd.addFamily(hcdCode);
                admin.createTable(htd);
            }

            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                admin.close();
            }
        }
    }

    private static void fetchUsers() throws IOException {
        System.out.println("Fetching users from `http://codeforces.com/api/user.ratedList`");
        Table table = null;
        try {
            String filename = BASE_DATA_DIR + "user" + File.separator + "list";
//            FileUtil.downloadURL("http://codeforces.com/api/user.ratedList", filename);
            Reader reader = new InputStreamReader(new FileInputStream(filename), "UTF-8");
            UserResponse userResponse = gson.fromJson(reader, UserResponse.class);
            List<User> users = userResponse.getResult();

            table = conn.getTable(TableName.valueOf("codeforces:user"));
            for (User user : users) {
                System.out.println("Put to HBase: " + user.getHandle() + ": " + user.getEmail() + " " + user.getRank() + " " + user.getRating());
                /**
                 * Table            codeforces:user
                 *
                 * Row key          {handle}
                 * Column Family 1  info
                 * Columns          email, vkId, openId, firstName, lastName, country, city, organization,
                 *                  contribution, lastOnlineTimeSeconds, registrationTimeSeconds
                 *
                 * Column Family 2  rank
                 * Columns          rank, rating, maxRank, maxRating
                 */
                Put put = new Put(user.getHandle().getBytes());

                if (user.getEmail() != null) put.addColumn("info".getBytes(), "email".getBytes(), ByteUtil.toByteArray(user.getEmail()));
                if (user.getVkId() != null) put.addColumn("info".getBytes(), "vkId".getBytes(), ByteUtil.toByteArray(user.getVkId()));
                if (user.getOpenId() != null) put.addColumn("info".getBytes(), "openId".getBytes(), ByteUtil.toByteArray(user.getOpenId()));
                if (user.getFirstName() != null) put.addColumn("info".getBytes(), "firstName".getBytes(), ByteUtil.toByteArray(user.getFirstName()));
                if (user.getLastName() != null) put.addColumn("info".getBytes(), "lastName".getBytes(), ByteUtil.toByteArray(user.getLastName()));
                if (user.getCountry() != null) put.addColumn("info".getBytes(), "country".getBytes(), ByteUtil.toByteArray(user.getCountry()));
                if (user.getCity() != null) put.addColumn("info".getBytes(), "city".getBytes(), ByteUtil.toByteArray(user.getCity()));
                if (user.getOrganization() != null) put.addColumn("info".getBytes(), "organization".getBytes(), ByteUtil.toByteArray(user.getOrganization()));
                if (user.getContribution() != null) put.addColumn("info".getBytes(), "contribution".getBytes(), ByteUtil.toByteArray(user.getContribution()));
                if (user.getLastOnlineTimeSeconds() != null) put.addColumn("info".getBytes(), "lastOnlineTimeSeconds".getBytes(), ByteUtil.toByteArray(user.getLastOnlineTimeSeconds()));
                if (user.getRegistrationTimeSeconds() != null) put.addColumn("info".getBytes(), "registrationTimeSeconds".getBytes(), ByteUtil.toByteArray(user.getRegistrationTimeSeconds()));

                if (user.getRank() != null) put.addColumn("rank".getBytes(), "rank".getBytes(), ByteUtil.toByteArray(user.getRank()));
                if (user.getRating() != null) put.addColumn("rank".getBytes(), "rating".getBytes(), ByteUtil.toByteArray(user.getRating()));
                if (user.getMaxRank() != null) put.addColumn("rank".getBytes(), "maxRank".getBytes(), ByteUtil.toByteArray(user.getMaxRank()));
                if (user.getMaxRating() != null) put.addColumn("rank".getBytes(), "maxRating".getBytes(), ByteUtil.toByteArray(user.getMaxRating()));

                table.put(put);

                //fetchUserSubmissions(user.getHandle());
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    private static void fetchProblems() throws IOException {
        System.out.println("Fetching problems from `http://codeforces.com/api/problemset.problems`");
        Table table = null;
        try {
            String filename = BASE_DATA_DIR + "problem" + File.separator + "list";
//            FileUtil.downloadURL("http://codeforces.com/api/problemset.problems", filename);
            Reader reader = new InputStreamReader(new FileInputStream(filename), "UTF-8");
            ProblemResponse problemResponse = gson.fromJson(reader, ProblemResponse.class);
            ProblemResponseResult result = problemResponse.getResult();

            table = conn.getTable(TableName.valueOf("codeforces:problem"));
            for (Problem problem : result.getProblems()) {
                System.out.println("Put to HBase: " + problem.getContestId() + "-" + problem.getIndex() + ": " + problem.getName());

                /**
                 * Table            codeforces:problem
                 *
                 * Row key          {contestId}-{index}
                 * Column Family 1  info
                 * Columns          name, type, points, tags, solvedCount
                 *
                 * Column Family 2  html
                 * Columns          content, timeLimit, memoryLimit, ...
                 */
                Put put = new Put((problem.getContestId() + "-" + problem.getIndex()).getBytes());

                if (problem.getName() != null) put.addColumn("info".getBytes(), "name".getBytes(), ByteUtil.toByteArray(problem.getName()));
                if (problem.getType() != null) put.addColumn("info".getBytes(), "type".getBytes(), ByteUtil.toByteArray(problem.getType()));
                if (problem.getPoints() != null) put.addColumn("info".getBytes(), "points".getBytes(), ByteUtil.toByteArray(problem.getPoints()));
                if (problem.getTags() != null) put.addColumn("info".getBytes(), "tags".getBytes(), ByteUtil.toByteArray(problem.getTags()));

                table.put(put);
            }
            for (ProblemStatistics problemStatistics : result.getProblemStatistics()) {
                System.out.println("Put to HBase: " + problemStatistics.getContestId() + "-" + problemStatistics.getIndex() + ": " + problemStatistics.getSolvedCount());

                Put put = new Put((problemStatistics.getContestId() + "-" + problemStatistics.getIndex()).getBytes());

                if (problemStatistics.getSolvedCount() != null) put.addColumn("info".getBytes(), "solvedCount".getBytes(), ByteUtil.toByteArray(problemStatistics.getSolvedCount()));

                table.put(put);
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    private static void fetchContest(boolean gym) throws IOException {
        String url = "http://codeforces.com/api/contest.list" + (gym ? "?gym=true" : "");
        System.out.println("Fetching " + (gym ? "gym" : "") + " contests from `" + url + "`");
        Table table = null;
        try {
            String filename = BASE_DATA_DIR + "contest" + File.separator + "list" + (gym ? "_gym" : "_regular");
//            FileUtil.downloadURL(url, filename);
            Reader reader = new InputStreamReader(new FileInputStream(filename), "UTF-8");
            ContestResponse contestResponse = gson.fromJson(reader, ContestResponse.class);
            List<Contest> contests = contestResponse.getResult();

            table = conn.getTable(TableName.valueOf("codeforces:contest"));
            for (Contest contest : contests) {
//                if (contest.getId() > 147) continue;
                System.out.println("Put to HBase: " + contest.getId() + ": " + contest.getName() + " " + contest.getStartTimeSeconds());

                /**
                 * Table            codeforces:contest
                 *
                 * Row key          {id}
                 * Column Family 1  info
                 * Columns          name, type, phase, frozen, preparedBy, websiteUrl, description,
                 *                  difficulty, kind, icpcRegion, city, season
                 *
                 * Column Family 2  time
                 * Columns          startTimeSeconds, durationSeconds, relativeTimeSeconds
                 */
                Put put = new Put(String.valueOf(contest.getId()).getBytes());

                if (contest.getName() != null) put.addColumn("info".getBytes(), "name".getBytes(), ByteUtil.toByteArray(contest.getName()));
                if (contest.getType() != null) put.addColumn("info".getBytes(), "type".getBytes(), ByteUtil.toByteArray(contest.getType()));
                if (contest.getPhase() != null) put.addColumn("info".getBytes(), "phase".getBytes(), ByteUtil.toByteArray(contest.getPhase()));
                if (contest.getFrozen() != null) put.addColumn("info".getBytes(), "type".getBytes(), ByteUtil.toByteArray(contest.getFrozen()));
                if (contest.getPreparedBy() != null) put.addColumn("info".getBytes(), "preparedBy".getBytes(), ByteUtil.toByteArray(contest.getPreparedBy()));
                if (contest.getWebsiteUrl() != null) put.addColumn("info".getBytes(), "websiteUrl".getBytes(), ByteUtil.toByteArray(contest.getWebsiteUrl()));
                if (contest.getDescription() != null) put.addColumn("info".getBytes(), "description".getBytes(), ByteUtil.toByteArray(contest.getDescription()));
                if (contest.getDifficulty() != null) put.addColumn("info".getBytes(), "difficulty".getBytes(), ByteUtil.toByteArray(contest.getDifficulty()));
                if (contest.getKind() != null) put.addColumn("info".getBytes(), "kind".getBytes(), ByteUtil.toByteArray(contest.getKind()));
                if (contest.getIcpcRegion() != null) put.addColumn("info".getBytes(), "icpcRegion".getBytes(), ByteUtil.toByteArray(contest.getIcpcRegion()));
                if (contest.getCity() != null) put.addColumn("info".getBytes(), "city".getBytes(), ByteUtil.toByteArray(contest.getCity()));
                if (contest.getSeason() != null) put.addColumn("info".getBytes(), "season".getBytes(), ByteUtil.toByteArray(contest.getSeason()));

                if (contest.getStartTimeSeconds() != null) put.addColumn("time".getBytes(), "startTimeSeconds".getBytes(), ByteUtil.toByteArray(contest.getStartTimeSeconds()));
                if (contest.getDurationSeconds() != null) put.addColumn("time".getBytes(), "durationSeconds".getBytes(), ByteUtil.toByteArray(contest.getDurationSeconds()));
                if (contest.getRelativeTimeSeconds() != null) put.addColumn("time".getBytes(), "relativeTimeSeconds".getBytes(), ByteUtil.toByteArray(contest.getRelativeTimeSeconds()));

                table.put(put);

                if (contest.getPhase() != Contest.Phase.BEFORE) {
                    fetchContestSubmission(contest.getId());
                }
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    private static void fetchContestSubmission(Integer id) {
        try {
            String filename = BASE_DATA_DIR + "submission" + File.separator + "contest" + File.separator + id;
//            boolean done = false;
            SubmissionResponse submissionResponse = new SubmissionResponse();
//            while (!done) {
//                FileUtil.downloadURL("http://codeforces.com/api/contest.status?contestId=" + id, filename);
                Reader reader = new InputStreamReader(new FileInputStream(filename), "UTF-8");
                try {
                    submissionResponse = gson.fromJson(reader, SubmissionResponse.class);
//                    done = true;
                } catch (JsonSyntaxException e) {
                    System.out.println("MalformedJsonException!, retrying ...");
//                    File file = new File(filename);
//                    file.delete();
                }
//            }
            while (DataProvider.unPutSubmissionQueue.size() > 10000) {
                System.out.println("Waiting: " + DataProvider.unPutSubmissionQueue.size());
                Thread.sleep(2000);
            }
            DataProvider.unPutSubmissionQueue.addAll(submissionResponse.getResult());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void fetchUserSubmissions(String handle) {
        try {
            String filename = BASE_DATA_DIR + "submission" + File.separator + "user" + File.separator + handle;
//            FileUtil.downloadURL("http://codeforces.com/api/user.status?handle=" + handle, filename);
            Reader reader = new InputStreamReader(new FileInputStream(filename), "UTF-8");
            SubmissionResponse submissionResponse = gson.fromJson(reader, SubmissionResponse.class);
            DataProvider.unPutSubmissionQueue.addAll(submissionResponse.getResult());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        initFilesystem();

        initHBase();

        new Thread(new PutSubmission(conn)).start();

        fetchProblems();

        fetchContest(true);  // gym contests
        fetchContest(false);  // regular contests

        fetchUsers();
    }

    private static void initFilesystem() {
        System.out.println("Checking filesystem ... ");
        File file = new File(BASE_DATA_DIR + "user" + File.separator);
        System.out.println("data/user/: " + file.mkdirs());
        file = new File(BASE_DATA_DIR + "contest" + File.separator);
        System.out.println("data/contest/: " + file.mkdirs());
        file = new File(BASE_DATA_DIR + "problem" + File.separator);
        System.out.println("data/problem/: " + file.mkdirs());
        file = new File(BASE_DATA_DIR + "submission" + File.separator);
        System.out.println("data/submission/: " + file.mkdirs());
        file = new File(BASE_DATA_DIR + "submission" + File.separator + "user" + File.separator);
        System.out.println("data/submission/user: " + file.mkdirs());
        file = new File(BASE_DATA_DIR + "submission" + File.separator + "contest" + File.separator);
        System.out.println("data/submission/contest: " + file.mkdirs());
    }

}
