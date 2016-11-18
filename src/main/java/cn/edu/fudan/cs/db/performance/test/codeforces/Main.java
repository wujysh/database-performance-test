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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.MalformedURLException;
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
                if (user.getRank() != null) put.addColumn("info".getBytes(), "rank".getBytes(), ByteUtil.toByteArray(user.getRank()));
                if (user.getMaxRank() != null) put.addColumn("info".getBytes(), "maxRank".getBytes(), ByteUtil.toByteArray(user.getMaxRank()));

                if (user.getRating() != null) put.addColumn("rating".getBytes(), "rating".getBytes(), ByteUtil.toByteArray(user.getRating()));
                if (user.getMaxRating() != null) put.addColumn("rating".getBytes(), "maxRating".getBytes(), ByteUtil.toByteArray(user.getMaxRating()));

                putList.add(put);
                //fetchUserSubmissions(user.getHandle());
            }

            long startTime = System.currentTimeMillis();
            table.put(putList);
            long endTime = System.currentTimeMillis();
            logger.info("Putting {} users into HBase costs {} ms.", users.size(), endTime - startTime);
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

                Put put = new Put((problem.getContestIdWithPaddingZero() + "-" + problem.getIndex()).getBytes());

                if (problem.getName() != null) put.addColumn("info".getBytes(), "name".getBytes(), ByteUtil.toByteArray(problem.getName()));
                if (problem.getType() != null) put.addColumn("info".getBytes(), "type".getBytes(), ByteUtil.toByteArray(problem.getType()));
                if (problem.getPoints() != null) put.addColumn("info".getBytes(), "points".getBytes(), ByteUtil.toByteArray(problem.getPoints()));
                if (problem.getTags() != null) put.addColumn("info".getBytes(), "tags".getBytes(), ByteUtil.toByteArray(problem.getTags()));

                putList.add(put);
            }
            long startTime = System.currentTimeMillis();
            table.put(putList);
            long endTime = System.currentTimeMillis();
            logger.info("Putting {} problems into HBase costs {} ms.", result.getProblems().size(), endTime - startTime);

            putList = new ArrayList<>(result.getProblemStatistics().size());
            for (ProblemStatistics problemStatistics : result.getProblemStatistics()) {
                System.out.println("Put to HBase: " + problemStatistics.getContestId() + "-" + problemStatistics.getIndex() + ": " + problemStatistics.getSolvedCount());

                Put put = new Put((problemStatistics.getContestIdWithPaddingZero() + "-" + problemStatistics.getIndex()).getBytes());

                if (problemStatistics.getSolvedCount() != null) put.addColumn("info".getBytes(), "solvedCount".getBytes(), ByteUtil.toByteArray(problemStatistics.getSolvedCount()));

                putList.add(put);
            }
            startTime = System.currentTimeMillis();
            table.put(putList);
            endTime = System.currentTimeMillis();
            logger.info("Putting {} problem statistics into HBase costs {} ms.", result.getProblemStatistics().size(), endTime - startTime);

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
                Put put = new Put(contest.getIdWithPaddingZero().getBytes());

                if (contest.getName() != null)
                    put.addColumn("info".getBytes(), "name".getBytes(), ByteUtil.toByteArray(contest.getName()));
                if (contest.getType() != null)
                    put.addColumn("info".getBytes(), "type".getBytes(), ByteUtil.toByteArray(contest.getType()));
                if (contest.getPhase() != null)
                    put.addColumn("info".getBytes(), "phase".getBytes(), ByteUtil.toByteArray(contest.getPhase()));
                if (contest.getFrozen() != null)
                    put.addColumn("info".getBytes(), "frozen".getBytes(), ByteUtil.toByteArray(contest.getFrozen()));

                if (contest.getStartTimeSeconds() != null)
                    put.addColumn("time".getBytes(), "startTimeSeconds".getBytes(), ByteUtil.toByteArray(contest.getStartTimeSeconds()));
                if (contest.getDurationSeconds() != null)
                    put.addColumn("time".getBytes(), "durationSeconds".getBytes(), ByteUtil.toByteArray(contest.getDurationSeconds()));
                if (contest.getRelativeTimeSeconds() != null)
                    put.addColumn("time".getBytes(), "relativeTimeSeconds".getBytes(), ByteUtil.toByteArray(contest.getRelativeTimeSeconds()));

                if (contest.getPreparedBy() != null)
                    put.addColumn("other".getBytes(), "preparedBy".getBytes(), ByteUtil.toByteArray(contest.getPreparedBy()));
                if (contest.getWebsiteUrl() != null)
                    put.addColumn("other".getBytes(), "websiteUrl".getBytes(), ByteUtil.toByteArray(contest.getWebsiteUrl()));
                if (contest.getDescription() != null)
                    put.addColumn("other".getBytes(), "description".getBytes(), ByteUtil.toByteArray(contest.getDescription()));
                if (contest.getDifficulty() != null)
                    put.addColumn("other".getBytes(), "difficulty".getBytes(), ByteUtil.toByteArray(contest.getDifficulty()));
                if (contest.getKind() != null)
                    put.addColumn("other".getBytes(), "kind".getBytes(), ByteUtil.toByteArray(contest.getKind()));
                if (contest.getIcpcRegion() != null)
                    put.addColumn("other".getBytes(), "icpcRegion".getBytes(), ByteUtil.toByteArray(contest.getIcpcRegion()));
                if (contest.getCity() != null)
                    put.addColumn("other".getBytes(), "city".getBytes(), ByteUtil.toByteArray(contest.getCity()));
                if (contest.getSeason() != null)
                    put.addColumn("other".getBytes(), "season".getBytes(), ByteUtil.toByteArray(contest.getSeason()));

                putList.add(put);
            }

            long startTime = System.currentTimeMillis();
            table.put(putList);
            long endTime = System.currentTimeMillis();
            logger.info("Putting {} contests into HBase costs {} ms.", contests.size(), endTime - startTime);

            for (Contest contest : contests) {
                System.out.println("Put to HBase: " + contest.getId() + ": " + contest.getName() + " " + contest.getStartTimeSeconds());

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
            boolean done = false;
            SubmissionResponse submissionResponse = new SubmissionResponse();
            while (!done) {
//                FileUtil.downloadURL("http://codeforces.com/api/contest.status?contestId=" + id, filename);
                Reader reader = new InputStreamReader(new FileInputStream(filename), "UTF-8");
                try {
                    submissionResponse = gson.fromJson(reader, SubmissionResponse.class);
                    done = true;
                } catch (JsonSyntaxException e) {
                    System.out.println("MalformedJsonException!, retrying ...");
                    Thread.sleep(2000);
                    FileUtil.downloadURL("http://codeforces.com/api/contest.status?contestId=" + id, filename, true);
                }
            }
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
