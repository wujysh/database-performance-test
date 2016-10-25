package cn.edu.fudan.cs.db.performance.test.codeforces.entity;

/**
 * Created by wujy on 16-1-16.
 */

/**
 * Represents a hack, made during Codeforces Round.
 *
 * Field	            Description
 * id	                Integer.
 * creationTimeSeconds	Integer. Hack creation time in unix format.
 * hacker	            Party object.
 * defender	            Party object.
 * verdict	            Enum: HACK_SUCCESSFUL, HACK_UNSUCCESSFUL, INVALID_INPUT, GENERATOR_INCOMPILABLE, GENERATOR_CRASHED, IGNORED, TESTING, OTHER. Can be absent.
 * problem	            Problem object. Hacked problem.
 * test	                String. Can be absent.
 * judgeProtocol	    Object with three fields: "manual", "protocol" and "verdict". Field manual can have values "true" and "false". If manual is "true" then test for the hack was entered manually. Fields "protocol" and "verdict" contain human-readable description of judge protocol and hack verdict. Localized. Can be absent.
 */
public class Hack {

    private Integer id;

    private Integer creationTimeSeconds;

    private Party hacker;

    private Party defender;

    private enum Verdict {
        HACK_SUCCESSFUL, HACK_UNSUCCESSFUL, INVALID_INPUT, GENERATOR_INCOMPILABLE, GENERATOR_CRASHED, IGNORED, TESTING, OTHER
    }
    private Verdict verdict;

    private Problem problem;

    private String test;

    private class JudgeProtocol {

        private Boolean manual;

        private String protocol;

        private String verdict;

        public Boolean getManual() {
            return manual;
        }

        public void setManual(Boolean manual) {
            this.manual = manual;
        }

        public String getProtocol() {
            return protocol;
        }

        public void setProtocol(String protocol) {
            this.protocol = protocol;
        }

        public String getVerdict() {
            return verdict;
        }

        public void setVerdict(String verdict) {
            this.verdict = verdict;
        }

    }
    private JudgeProtocol judgeProtocol;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getCreationTimeSeconds() {
        return creationTimeSeconds;
    }

    public void setCreationTimeSeconds(Integer creationTimeSeconds) {
        this.creationTimeSeconds = creationTimeSeconds;
    }

    public Party getHacker() {
        return hacker;
    }

    public void setHacker(Party hacker) {
        this.hacker = hacker;
    }

    public Party getDefender() {
        return defender;
    }

    public void setDefender(Party defender) {
        this.defender = defender;
    }

    public Verdict getVerdict() {
        return verdict;
    }

    public void setVerdict(Verdict verdict) {
        this.verdict = verdict;
    }

    public Problem getProblem() {
        return problem;
    }

    public void setProblem(Problem problem) {
        this.problem = problem;
    }

    public String getTest() {
        return test;
    }

    public void setTest(String test) {
        this.test = test;
    }

    public JudgeProtocol getJudgeProtocol() {
        return judgeProtocol;
    }

    public void setJudgeProtocol(JudgeProtocol judgeProtocol) {
        this.judgeProtocol = judgeProtocol;
    }

}
