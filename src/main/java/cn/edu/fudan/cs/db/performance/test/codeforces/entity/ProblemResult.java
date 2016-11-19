package cn.edu.fudan.cs.db.performance.test.codeforces.entity;

/**
 * Created by wujy on 16-1-16.
 */

/**
 * Represents a submissions results of a party for a problem.
 *
 * Field	                    Description
 * points	                    Floating point number.
 * penalty	                    Integer. Penalty (in ICPC meaning) of the party for this problem.
 * rejectedAttemptCount         Integer. Number of incorrect submissions.
 * type                         Enum: PRELIMINARY, FINAL. If type is PRELIMINARY then points can decrease (if, for example, solution will fail during system test). Otherwise, party can only increase points for this problem by submitting better solutions.
 * bestSubmissionTimeSeconds    Integer. Number of seconds after the start of the contest before the submission, that brought maximal amount of points for this problem.
 */
public class ProblemResult {

    private Float points;

    private Integer penalty;

    private Integer rejectedAttemptCount;

    public enum Type {
        PRELIMINARY, FINAL
    }
    private Type type;

    private Integer bestSubmissionTimeSeconds;

    public Float getPoints() {
        return points;
    }

    public void setPoints(Float points) {
        this.points = points;
    }

    public Integer getPenalty() {
        return penalty;
    }

    public void setPenalty(Integer penalty) {
        this.penalty = penalty;
    }

    public Integer getRejectedAttemptCount() {
        return rejectedAttemptCount;
    }

    public void setRejectedAttemptCount(Integer rejectedAttemptCount) {
        this.rejectedAttemptCount = rejectedAttemptCount;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Integer getBestSubmissionTimeSeconds() {
        return bestSubmissionTimeSeconds;
    }

    public void setBestSubmissionTimeSeconds(Integer bestSubmissionTimeSeconds) {
        this.bestSubmissionTimeSeconds = bestSubmissionTimeSeconds;
    }

}
