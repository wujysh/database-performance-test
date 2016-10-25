package cn.edu.fudan.cs.db.performance.test.codeforces.entity;

/**
 * Created by wujy on 16-1-16.
 */

/**
 * Represents a Codeforces user.
 *
 * Field	                Description
 * handle	                String. Codeforces user handle.
 * email	                String. Shown only if user allowed to share his contact info.
 * vkId	                    String. User id for VK social network. Shown only if user allowed to share his contact info.
 * openId	                String. Shown only if user allowed to share his contact info.
 * firstName	            String. Localized. Can be absent.
 * lastName	                String. Localized. Can be absent.
 * country	                String. Localized. Can be absent.
 * city	                    String. Localized. Can be absent.
 * organization	            String. Localized. Can be absent.
 * contribution	            Integer. User contribution.
 * rank	                    String. Localized.
 * rating	                Integer.
 * maxRank	                String. Localized.
 * maxRating	            Integer.
 * lastOnlineTimeSeconds    Integer. Time, when user was last seen online, in unix format.
 * registrationTimeSeconds  Integer. Time, when user was registered, in unix format.
 */
public class User {

    private String handle;

    private String email;

    private String vkId;

    private String openId;

    private String firstName;

    private String lastName;

    private String country;

    private String city;

    private String organization;

    private Integer contribution;

    private String rank;

    private Integer rating;

    private String maxRank;

    private Integer maxRating;

    private Integer lastOnlineTimeSeconds;

    private Integer registrationTimeSeconds;

    public String getHandle() {
        return handle;
    }

    public void setHandle(String handle) {
        this.handle = handle;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getVkId() {
        return vkId;
    }

    public void setVkId(String vkId) {
        this.vkId = vkId;
    }

    public String getOpenId() {
        return openId;
    }

    public void setOpenId(String openId) {
        this.openId = openId;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
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

    public String getOrganization() {
        return organization;
    }

    public void setOrganization(String organization) {
        this.organization = organization;
    }

    public Integer getContribution() {
        return contribution;
    }

    public void setContribution(Integer contribution) {
        this.contribution = contribution;
    }

    public String getRank() {
        return rank;
    }

    public void setRank(String rank) {
        this.rank = rank;
    }

    public Integer getRating() {
        return rating;
    }

    public void setRating(Integer rating) {
        this.rating = rating;
    }

    public String getMaxRank() {
        return maxRank;
    }

    public void setMaxRank(String maxRank) {
        this.maxRank = maxRank;
    }

    public Integer getMaxRating() {
        return maxRating;
    }

    public void setMaxRating(Integer maxRating) {
        this.maxRating = maxRating;
    }

    public Integer getLastOnlineTimeSeconds() {
        return lastOnlineTimeSeconds;
    }

    public void setLastOnlineTimeSeconds(Integer lastOnlineTimeSeconds) {
        this.lastOnlineTimeSeconds = lastOnlineTimeSeconds;
    }

    public Integer getRegistrationTimeSeconds() {
        return registrationTimeSeconds;
    }

    public void setRegistrationTimeSeconds(Integer registrationTimeSeconds) {
        this.registrationTimeSeconds = registrationTimeSeconds;
    }

}
