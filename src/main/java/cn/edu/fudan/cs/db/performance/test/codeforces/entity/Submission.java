package cn.edu.fudan.cs.db.performance.test.codeforces.entity;

/**
 * Created by wujy on 16-1-16.
 */

/**
 * Represents a submission.
 *
 * Field	                Description
 * id	                    Integer.
 * contestId	            Integer.
 * creationTimeSeconds	    Integer. Time, when submission was created, in unix-format.
 * relativeTimeSeconds	    Integer. Number of seconds, passed after the start of the contest (or a virtual start for virtual parties), before the submission.
 * problem	                Problem object.
 * author	                Party object.
 * programmingLanguage	    String.
 * verdict	                Enum: FAILED, OK, PARTIAL, COMPILATION_ERROR, RUNTIME_ERROR, WRONG_ANSWER, PRESENTATION_ERROR, TIME_LIMIT_EXCEEDED, MEMORY_LIMIT_EXCEEDED, IDLENESS_LIMIT_EXCEEDED, SECURITY_VIOLATED, CRASHED, INPUT_PREPARATION_CRASHED, CHALLENGED, SKIPPED, TESTING, REJECTED. Can be absent.
 * testset	                Enum: SAMPLES, PRETESTS, TESTS, CHALLENGES, TESTS1, ..., TESTS10. Testset used for judging the submission.
 * passedTestCount	        Integer. Number of passed tests.
 * timeConsumedMillis       Integer. Maximum time in milliseconds, consumed by solution for one test.
 * memoryConsumedBytes	    Integer. Maximum memory in bytes, consumed by solution for one test.
 */
public class Submission {

    private Integer id;

    private Integer contestId;

    private Integer creationTimeSeconds;

    private Integer relativeTimeSeconds;

    private Problem problem;

    private Party author;

    private String programmingLanguage;

    private enum Verdict {
        FAILED, OK, PARTIAL, COMPILATION_ERROR, RUNTIME_ERROR, WRONG_ANSWER, PRESENTATION_ERROR, TIME_LIMIT_EXCEEDED, MEMORY_LIMIT_EXCEEDED, IDLENESS_LIMIT_EXCEEDED, SECURITY_VIOLATED, CRASHED, INPUT_PREPARATION_CRASHED, CHALLENGED, SKIPPED, TESTING, REJECTED
    }
    private Verdict verdict;

    private enum Testset {
        SAMPLES, PRETESTS, TESTS, CHALLENGES, TESTS1, TESTS2, TESTS3, TESTS4, TESTS5, TESTS6, TESTS7, TESTS8, TESTS9, TESTS10
    }
    private Testset testset;

    private Integer passedTestCount;

    private Integer timeConsumedMillis;

    private Integer memoryConsumedBytes;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getContestId() {
        return contestId;
    }

    public void setContestId(Integer contestId) {
        this.contestId = contestId;
    }

    public Integer getCreationTimeSeconds() {
        return creationTimeSeconds;
    }

    public void setCreationTimeSeconds(Integer creationTimeSeconds) {
        this.creationTimeSeconds = creationTimeSeconds;
    }

    public Integer getRelativeTimeSeconds() {
        return relativeTimeSeconds;
    }

    public void setRelativeTimeSeconds(Integer relativeTimeSeconds) {
        this.relativeTimeSeconds = relativeTimeSeconds;
    }

    public Problem getProblem() {
        return problem;
    }

    public void setProblem(Problem problem) {
        this.problem = problem;
    }

    public Party getAuthor() {
        return author;
    }

    public void setAuthor(Party author) {
        this.author = author;
    }

    public String getProgrammingLanguage() {
        return programmingLanguage;
    }

    public void setProgrammingLanguage(String programmingLanguage) {
        this.programmingLanguage = programmingLanguage;
    }

    public Verdict getVerdict() {
        return verdict;
    }

    public void setVerdict(Verdict verdict) {
        this.verdict = verdict;
    }

    public Testset getTestset() {
        return testset;
    }

    public void setTestset(Testset testset) {
        this.testset = testset;
    }

    public Integer getPassedTestCount() {
        return passedTestCount;
    }

    public void setPassedTestCount(Integer passedTestCount) {
        this.passedTestCount = passedTestCount;
    }

    public Integer getTimeConsumedMillis() {
        return timeConsumedMillis;
    }

    public void setTimeConsumedMillis(Integer timeConsumedMillis) {
        this.timeConsumedMillis = timeConsumedMillis;
    }

    public Integer getMemoryConsumedBytes() {
        return memoryConsumedBytes;
    }

    public void setMemoryConsumedBytes(Integer memoryConsumedBytes) {
        this.memoryConsumedBytes = memoryConsumedBytes;
    }

}
