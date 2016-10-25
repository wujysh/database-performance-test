package cn.edu.fudan.cs.db.performance.test.codeforces.entity;

/**
 * Created by wujy on 16-1-16.
 */

/**
 * Represents a participation of user in rated contest.
 *
 * Field	                Description
 * contestId	            Integer.
 * contestName	            String. Localized.
 * handle	                String. Codeforces user handle.
 * rank	                    Integer. Place of the user in the contest. This field contains user rank on the moment of rating update. If afterwards rank changes (e.g. someone get disqualified), this field will not be update and will contain old rank.
 * ratingUpdateTimeSeconds	Integer. Time, when rating for the contest was update, in unix-format.
 * oldRating	            Integer. User rating before the contest.
 * newRating	            Integer. User rating after the contest.
 */
public class RatingChange {

    private Integer contestId;

    private String contestName;

    private String handle;

    private Integer rank;

    private Integer ratingUpdateTimeSeconds;

    private Integer oldRating;

    private Integer newRating;

    public Integer getContestId() {
        return contestId;
    }

    public void setContestId(Integer contestId) {
        this.contestId = contestId;
    }

    public String getContestName() {
        return contestName;
    }

    public void setContestName(String contestName) {
        this.contestName = contestName;
    }

    public String getHandle() {
        return handle;
    }

    public void setHandle(String handle) {
        this.handle = handle;
    }

    public Integer getRank() {
        return rank;
    }

    public void setRank(Integer rank) {
        this.rank = rank;
    }

    public Integer getRatingUpdateTimeSeconds() {
        return ratingUpdateTimeSeconds;
    }

    public void setRatingUpdateTimeSeconds(Integer ratingUpdateTimeSeconds) {
        this.ratingUpdateTimeSeconds = ratingUpdateTimeSeconds;
    }

    public Integer getOldRating() {
        return oldRating;
    }

    public void setOldRating(Integer oldRating) {
        this.oldRating = oldRating;
    }

    public Integer getNewRating() {
        return newRating;
    }

    public void setNewRating(Integer newRating) {
        this.newRating = newRating;
    }

}
