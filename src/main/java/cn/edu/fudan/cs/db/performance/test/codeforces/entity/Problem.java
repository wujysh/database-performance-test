package cn.edu.fudan.cs.db.performance.test.codeforces.entity;

/**
 * Created by wujy on 16-1-16.
 */

import java.util.List;

/**
 * Represents a problem.
 *
 * Field	        Description
 * contestId	    Integer. Id of the contest, containing the problem.
 * index	        String. Usually a letter of a letter, followed by a digit, that represent a problem index in a contest.
 * name	            String. Localized.
 * type	            Enum: PROGRAMMING, QUESTION.
 * points	        Floating point number. Can be absent. Maximum ammount of points for the problem.
 * tags	            String list. Problem tags.
 */
public class Problem {

    private Integer contestId;

    private String index;

    private String name;

    private enum Type {
        PROGRAMMING, QUESTION
    }
    private Type type;

    private Float points;

    private List<String> tags;

    public Integer getContestId() {
        return contestId;
    }

    public void setContestId(Integer contestId) {
        this.contestId = contestId;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Float getPoints() {
        return points;
    }

    public void setPoints(Float points) {
        this.points = points;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }
}
