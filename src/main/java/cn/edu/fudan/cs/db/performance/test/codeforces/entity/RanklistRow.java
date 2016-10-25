package cn.edu.fudan.cs.db.performance.test.codeforces.entity;

/**
 * Created by wujy on 16-1-16.
 */

import java.util.List;

/**
 * Represents a ranklist row.
 *
 * Field	                    Description
 * party	                    Party object. Party that took a corresponding place in the contest.
 * rank	                        Integer. Party place in the contest.
 * points	                    Floating point number. Total ammount of points, scored by the party.
 * penalty	                    Integer. Total penalty (in ICPC meaning) of the party.
 * successfulHackCount	        Integer.
 * unsuccessfulHackCount	    Integer.
 * problemResults	            List of ProblemResult objects. Party results for each problem. Order of the problems is the same as in "problems" field of the returned object.
 * lastSubmissionTimeSeconds    Integer. For IOI contests only. Time in seconds from the start of the contest to the last submission that added some points to the total score of the party.
 */
public class RanklistRow {

    private Party party;

    private Integer rank;

    private Float points;

    private Integer penalty;

    private Integer successfulHackCount;

    private Integer unsuccessfulHackCount;

    private List<ProblemResult> problemResults;

    private Integer lastSubmissionTimeSeconds;

    public Party getParty() {
        return party;
    }

    public void setParty(Party party) {
        this.party = party;
    }

    public Integer getRank() {
        return rank;
    }

    public void setRank(Integer rank) {
        this.rank = rank;
    }

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

    public Integer getSuccessfulHackCount() {
        return successfulHackCount;
    }

    public void setSuccessfulHackCount(Integer successfulHackCount) {
        this.successfulHackCount = successfulHackCount;
    }

    public Integer getUnsuccessfulHackCount() {
        return unsuccessfulHackCount;
    }

    public void setUnsuccessfulHackCount(Integer unsuccessfulHackCount) {
        this.unsuccessfulHackCount = unsuccessfulHackCount;
    }

    public List<ProblemResult> getProblemResults() {
        return problemResults;
    }

    public void setProblemResults(List<ProblemResult> problemResults) {
        this.problemResults = problemResults;
    }

    public Integer getLastSubmissionTimeSeconds() {
        return lastSubmissionTimeSeconds;
    }

    public void setLastSubmissionTimeSeconds(Integer lastSubmissionTimeSeconds) {
        this.lastSubmissionTimeSeconds = lastSubmissionTimeSeconds;
    }

}
