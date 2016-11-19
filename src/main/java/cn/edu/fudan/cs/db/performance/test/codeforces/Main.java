package cn.edu.fudan.cs.db.performance.test.codeforces;

import cn.edu.fudan.cs.db.performance.test.codeforces.entity.Contest;
import cn.edu.fudan.cs.db.performance.test.codeforces.entity.Problem;
import cn.edu.fudan.cs.db.performance.test.codeforces.entity.ProblemStatistics;
import cn.edu.fudan.cs.db.performance.test.codeforces.entity.User;
import cn.edu.fudan.cs.db.performance.test.codeforces.response.*;
import cn.edu.fudan.cs.db.performance.test.codeforces.thread.PutSubmission;
import cn.edu.fudan.cs.db.performance.test.util.ByteUtil;
import cn.edu.fudan.cs.db.performance.test.util.FileUtil;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wujy on 16-1-16.
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class.getName());

    private static Connection conn = null;

    private static Gson gson = new Gson();

    private static String BASE_DATA_DIR = "data" + File.separator;

    private static long cntSubmissions;

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

            /*
             * Table            codeforces:user
             *
             * Row key          {handle}
             * Column Family 1  info
             * Columns          email, vkId, openId, firstName, lastName, country, city, organization,
             *                  contribution, lastOnlineTimeSeconds, registrationTimeSeconds, rank, maxRank
             *
             * Column Family 2  rating
             * Columns          rating, maxRating
             */
            System.out.println("Checking HBase table `codeforces:user`");
            TableName tableNameUser = TableName.valueOf("codeforces:user");
            if (!admin.tableExists(tableNameUser)) {
                System.out.println("Creating HBase table `codeforces:user`");
                HTableDescriptor htd = new HTableDescriptor(tableNameUser);
                HColumnDescriptor hcdInfo = new HColumnDescriptor("info");
                HColumnDescriptor hcdRank = new HColumnDescriptor("rating");
                htd.addFamily(hcdInfo);
                htd.addFamily(hcdRank);
                admin.createTable(htd);
            }

            /*
             * Table            codeforces:contest
             *
             * Row key          {id}(padding)
             *
             * Column Family 1  info
             * Columns          name, type, phase, frozen
             *
             * Column Family 2  time
             * Columns          durationSeconds, startTimeSeconds, relativeTimeSeconds
             *
             * Column Family 3  other
             * Columns          preparedBy, websiteUrl, description,
             *                  difficulty, kind, icpcRegion, city, season
             */
            System.out.println("Checking HBase table `codeforces:contest`");
            TableName tableNameContest = TableName.valueOf("codeforces:contest");
            if (!admin.tableExists(tableNameContest)) {
                System.out.println("Creating HBase table `codeforces:contest`");
                HTableDescriptor htd = new HTableDescriptor(tableNameContest);
                HColumnDescriptor hcdInfo = new HColumnDescriptor("info");
                HColumnDescriptor hcdTime = new HColumnDescriptor("time");
                HColumnDescriptor hcdOther = new HColumnDescriptor("other");
                htd.addFamily(hcdInfo);
                htd.addFamily(hcdTime);
                htd.addFamily(hcdOther);
                admin.createTable(htd);
            }

            /*
             * Table            codeforces:problem
             *
             * Row key          {contestId}(padding)-{index}
             *
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
            System.out.println("Checking HBase table `codeforces:submission`");
            TableName tableNameSubmission = TableName.valueOf("codeforces:submission");
            if (!admin.tableExists(tableNameSubmission)) {
                System.out.println("Creating HBase table `codeforces:submission`");
                HTableDescriptor htd = new HTableDescriptor(tableNameSubmission);
                HColumnDescriptor hcdVerdict = new HColumnDescriptor("verdict");
                HColumnDescriptor hcdInfo = new HColumnDescriptor("info");
                HColumnDescriptor hcdCode = new HColumnDescriptor("code");
                htd.addFamily(hcdVerdict);
                htd.addFamily(hcdInfo);
                htd.addFamily(hcdCode);
                admin.createTable(htd);
            }

            admin.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
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
            List<Put> putList = new ArrayList<>(users.size());
            for (User user : users) {
                System.out.println("Put to HBase: " + user.getHandle() + ": " + user.getEmail() + " " + user.getRank() + " " + user.getRating());
                /*
                 * Table            codeforces:user
                 *
                 * Row key          {handle}
                 * Column Family 1  info
                 * Columns          email, vkId, openId, firstName, lastName, country, city, organization,
                 *                  contribution, lastOnlineTimeSeconds, registrationTimeSeconds, rank, maxRank
                 *
                 * Column Family 2  rating
                 * Columns          rating, maxRating
                 */
                Put put = new Put(Bytes.toBytes(user.getHandle()));

                if (user.getEmail() != null) put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("email"), Bytes.toBytes(user.getEmail()));
                if (user.getVkId() != null) put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("vkId"), Bytes.toBytes(user.getVkId()));
                if (user.getOpenId() != null) put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("openId"), Bytes.toBytes(user.getOpenId()));
                if (user.getFirstName() != null) put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("firstName"), Bytes.toBytes(user.getFirstName()));
                if (user.getLastName() != null) put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("lastName"), Bytes.toBytes(user.getLastName()));
                if (user.getCountry() != null) put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("country"), Bytes.toBytes(user.getCountry()));
                if (user.getCity() != null) put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("city"), Bytes.toBytes(user.getCity()));
                if (user.getOrganization() != null) put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("organization"), Bytes.toBytes(user.getOrganization()));
                if (user.getContribution() != null) put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("contribution"), Bytes.toBytes(user.getContribution()));
                if (user.getLastOnlineTimeSeconds() != null) put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("lastOnlineTimeSeconds"), Bytes.toBytes(user.getLastOnlineTimeSeconds()));
                if (user.getRegistrationTimeSeconds() != null) put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("registrationTimeSeconds"), Bytes.toBytes(user.getRegistrationTimeSeconds()));
                if (user.getRank() != null) put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rank"), Bytes.toBytes(user.getRank()));
                if (user.getMaxRank() != null) put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("maxRank"), Bytes.toBytes(user.getMaxRank()));

                if (user.getRating() != null) put.addColumn(Bytes.toBytes("rating"), Bytes.toBytes("rating"), Bytes.toBytes(user.getRating()));
                if (user.getMaxRating() != null) put.addColumn(Bytes.toBytes("rating"), Bytes.toBytes("maxRating"), Bytes.toBytes(user.getMaxRating()));

                putList.add(put);
                //fetchUserSubmissions(user.getHandle());
            }

            long startTime = System.currentTimeMillis();
            table.put(putList);
            long endTime = System.currentTimeMillis();
            logger.info("Putting {} users into HBase costs {} ms.", users.size(), endTime - startTime);
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
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
            List<Put> putList = new ArrayList<>(result.getProblems().size());
            for (Problem problem : result.getProblems()) {
                System.out.println("Put to HBase: " + problem.getContestId() + "-" + problem.getIndex() + ": " + problem.getName());

                /*
                 * Table            codeforces:problem
                 *
                 * Row key          {contestId}(padding)-{index}
                 *
                 * Column Family 1  info
                 * Columns          name, type, points, tags, solvedCount
                 *
                 * Column Family 2  html
                 * Columns          content, timeLimit, memoryLimit, ...
                 */

                Put put = new Put(Bytes.toBytes(problem.getContestIdWithPaddingZero() + "-" + problem.getIndex()));

                if (problem.getName() != null) put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(problem.getName()));
                if (problem.getType() != null) put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("type"), Bytes.toBytes(String.valueOf(problem.getType())));
                if (problem.getPoints() != null) put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("points"), Bytes.toBytes(problem.getPoints()));
                if (problem.getTags() != null) put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("tags"), ByteUtil.toByteArray(problem.getTags()));

                putList.add(put);
            }
            long startTime = System.currentTimeMillis();
            table.put(putList);
            long endTime = System.currentTimeMillis();
            logger.info("Putting {} problems into HBase costs {} ms.", result.getProblems().size(), endTime - startTime);

            putList = new ArrayList<>(result.getProblemStatistics().size());
            for (ProblemStatistics problemStatistics : result.getProblemStatistics()) {
                System.out.println("Put to HBase: " + problemStatistics.getContestId() + "-" + problemStatistics.getIndex() + ": " + problemStatistics.getSolvedCount());

                Put put = new Put(Bytes.toBytes(problemStatistics.getContestIdWithPaddingZero() + "-" + problemStatistics.getIndex()));

                if (problemStatistics.getSolvedCount() != null) put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("solvedCount"), Bytes.toBytes(problemStatistics.getSolvedCount()));

                putList.add(put);
            }
            startTime = System.currentTimeMillis();
            table.put(putList);
            endTime = System.currentTimeMillis();
            logger.info("Putting {} problem statistics into HBase costs {} ms.", result.getProblemStatistics().size(), endTime - startTime);

        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
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
            List<Put> putList = new ArrayList<>(contests.size());
            for (Contest contest : contests) {

                /*
                 * Table            codeforces:contest
                 *
                 * Row key          {id}(padding)
                 *
                 * Column Family 1  info
                 * Columns          name, type, phase, frozen
                 *
                 * Column Family 2  time
                 * Columns          durationSeconds, startTimeSeconds, relativeTimeSeconds
                 *
                 * Column Family 3  other
                 * Columns          preparedBy, websiteUrl, description,
                 *                  difficulty, kind, icpcRegion, city, season
                 */
                Put put = new Put(Bytes.toBytes(contest.getIdWithPaddingZero()));

                if (contest.getName() != null)
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(contest.getName()));
                if (contest.getType() != null)
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("type"), Bytes.toBytes(String.valueOf(contest.getType())));
                if (contest.getPhase() != null)
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("phase"), Bytes.toBytes(String.valueOf(contest.getPhase())));
                if (contest.getFrozen() != null)
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("frozen"), Bytes.toBytes(contest.getFrozen()));

                if (contest.getStartTimeSeconds() != null)
                    put.addColumn(Bytes.toBytes("time"), Bytes.toBytes("startTimeSeconds"), Bytes.toBytes(contest.getStartTimeSeconds()));
                if (contest.getDurationSeconds() != null)
                    put.addColumn(Bytes.toBytes("time"), Bytes.toBytes("durationSeconds"), Bytes.toBytes(contest.getDurationSeconds()));
                if (contest.getRelativeTimeSeconds() != null)
                    put.addColumn(Bytes.toBytes("time"), Bytes.toBytes("relativeTimeSeconds"), Bytes.toBytes(contest.getRelativeTimeSeconds()));

                if (contest.getPreparedBy() != null)
                    put.addColumn(Bytes.toBytes("other"), Bytes.toBytes("preparedBy"), Bytes.toBytes(contest.getPreparedBy()));
                if (contest.getWebsiteUrl() != null)
                    put.addColumn(Bytes.toBytes("other"), Bytes.toBytes("websiteUrl"), Bytes.toBytes(contest.getWebsiteUrl()));
                if (contest.getDescription() != null)
                    put.addColumn(Bytes.toBytes("other"), Bytes.toBytes("description"), Bytes.toBytes(contest.getDescription()));
                if (contest.getDifficulty() != null)
                    put.addColumn(Bytes.toBytes("other"), Bytes.toBytes("difficulty"), Bytes.toBytes(contest.getDifficulty()));
                if (contest.getKind() != null)
                    put.addColumn(Bytes.toBytes("other"), Bytes.toBytes("kind"), Bytes.toBytes(contest.getKind()));
                if (contest.getIcpcRegion() != null)
                    put.addColumn(Bytes.toBytes("other"), Bytes.toBytes("icpcRegion"), Bytes.toBytes(contest.getIcpcRegion()));
                if (contest.getCity() != null)
                    put.addColumn(Bytes.toBytes("other"), Bytes.toBytes("city"), Bytes.toBytes(contest.getCity()));
                if (contest.getSeason() != null)
                    put.addColumn(Bytes.toBytes("other"), Bytes.toBytes("season"), Bytes.toBytes(contest.getSeason()));

                putList.add(put);
            }

            long startTime = System.currentTimeMillis();
            table.put(putList);
            long endTime = System.currentTimeMillis();
            logger.info("Putting {} contests into HBase costs {} ms.", contests.size(), endTime - startTime);

            cntSubmissions = 0;
            startTime = System.currentTimeMillis();
            for (Contest contest : contests) {
                System.out.println("Put to HBase: " + contest.getId() + ": " + contest.getName() + " " + contest.getStartTimeSeconds());

                if (contest.getPhase() != Contest.Phase.BEFORE) {
                    fetchContestSubmission(contest.getId());
                }
            }
            endTime = System.currentTimeMillis();
            logger.info("Putting {} submissions into HBase costs {} ms.", cntSubmissions, endTime - startTime);

        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    private static void fetchContestSubmission(Integer id) {
        try {
            String filename = BASE_DATA_DIR + "submission" + File.separator + "contest" + File.separator + id;
            boolean done = false;
            SubmissionResponse submissionResponse = new SubmissionResponse();
            while (!done) {
//                FileUtil.downloadURL("http://codeforces.com/api/contest.status?contestId=" + id, filename);
                Reader reader = new InputStreamReader(new FileInputStream(filename), "UTF-8");
                try {
                    submissionResponse = gson.fromJson(reader, SubmissionResponse.class);
                    done = true;
                } catch (JsonSyntaxException e) {
                    logger.warn("MalformedJsonException!, retrying ...");
                    Thread.sleep(2000);
                    FileUtil.downloadURL("http://codeforces.com/api/contest.status?contestId=" + id, filename, true);
                }
            }
            while (DataProvider.unPutSubmissionQueue.size() > 10000) {
                System.out.println("Waiting: " + DataProvider.unPutSubmissionQueue.size());
                Thread.sleep(2000);
            }
            cntSubmissions += submissionResponse.getResult().size();
            DataProvider.unPutSubmissionQueue.addAll(submissionResponse.getResult());
        } catch (IOException | InterruptedException e) {
            logger.error(e.getMessage(), e.getCause());
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
            logger.error(e.getMessage(), e.getCause());
        }
    }

    public static void main(String[] args) throws IOException {
        initFilesystem();

        initHBase();

        fetchUsers();

        fetchProblems();

        new Thread(new PutSubmission(conn)).start();
        fetchContest(true);  // gym contests
        fetchContest(false);  // regular contests
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
