package cn.edu.fudan.cs.db.performance.test.codeforces.entity;

/**
 * Created by wujy on 16-1-16.
 */

/**
 * Represents a contest on Codeforces.
 *
 * Field	                Description
 * id	                    Integer.
 * name	                    String. Localized.
 * type	                    Enum: CF, IOI, ICPC. Scoring system used for the contest.
 * phase	                Enum: BEFORE, CODING, PENDING_SYSTEM_TEST, SYSTEM_TEST, FINISHED.
 * frozen	                Boolean. If true, then the ranklist for the contest is frozen and shows only submissions, created before freeze.
 * durationSeconds	        Integer. Duration of the contest in seconds.
 * startTimeSeconds	        Integer. Can be absent. Contest start time in unix format.
 * relativeTimeSeconds	    Integer. Can be absent. Number of seconds, passed after the start of the contest. Can be negative.
 * preparedBy	            String. Can be absent. Handle of the user, how created the contest.
 * websiteUrl	            String. Can be absent. URL for contest-related website.
 * description	            String. Localized. Can be absent.
 * difficulty	            Integer. Can be absent. From 1 to 5. Larger number means more difficult problems.
 * kind	                    String. Localized. Can be absent. Human-readable type of the contest from the following categories: Official ACM-ICPC Contest, Official School Contest, Opencup Contest, School/University/City/Region Championship, Training Camp Contest, Official International Personal Contest, Training Contest.
 * icpcRegion	            String. Localized. Can be absent. Name of the ICPC Region for official ACM-ICPC contests.
 * country	                String. Localized. Can be absent.
 * city	                    String. Localized. Can be absent.
 * season	                String. Can be absent.
 */
public class Contest {

    private Integer id;

    private String name;

    public enum Type {
        CF, IOI, ICPC
    }
    private Type type;

    public enum Phase {
        BEFORE, CODING, PENDING_SYSTEM_TEST, SYSTEM_TEST, FINISHED
    }
    private Phase phase;

    private Boolean frozen;

    private Integer durationSeconds;

    private Integer startTimeSeconds;

    private Integer relativeTimeSeconds;

    private String preparedBy;

    private String websiteUrl;

    private String description;

    private Integer difficulty;

    private String kind;

    private String icpcRegion;

    private String country;

    private String city;

    private String season;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
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

    public Phase getPhase() {
        return phase;
    }

    public void setPhase(Phase phase) {
        this.phase = phase;
    }

    public Boolean getFrozen() {
        return frozen;
    }

    public void setFrozen(Boolean frozen) {
        this.frozen = frozen;
    }

    public Integer getDurationSeconds() {
        return durationSeconds;
    }

    public void setDurationSeconds(Integer durationSeconds) {
        this.durationSeconds = durationSeconds;
    }

    public Integer getStartTimeSeconds() {
        return startTimeSeconds;
    }

    public void setStartTimeSeconds(Integer startTimeSeconds) {
        this.startTimeSeconds = startTimeSeconds;
    }

    public Integer getRelativeTimeSeconds() {
        return relativeTimeSeconds;
    }

    public void setRelativeTimeSeconds(Integer relativeTimeSeconds) {
        this.relativeTimeSeconds = relativeTimeSeconds;
    }

    public String getPreparedBy() {
        return preparedBy;
    }

    public void setPreparedBy(String preparedBy) {
        this.preparedBy = preparedBy;
    }

    public String getWebsiteUrl() {
        return websiteUrl;
    }

    public void setWebsiteUrl(String websiteUrl) {
        this.websiteUrl = websiteUrl;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Integer getDifficulty() {
        return difficulty;
    }

    public void setDifficulty(Integer difficulty) {
        this.difficulty = difficulty;
    }

    public String getKind() {
        return kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    public String getIcpcRegion() {
        return icpcRegion;
    }

    public void setIcpcRegion(String icpcRegion) {
        this.icpcRegion = icpcRegion;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getSeason() {
        return season;
    }

    public void setSeason(String season) {
        this.season = season;
    }

}
