package cn.edu.fudan.cs.db.performance.test.codeforces.entity;

/**
 * Created by wujy on 16-1-16.
 */

import cn.edu.fudan.cs.db.performance.test.codeforces.entity.enums.ParticipantType;

import java.util.List;

/**
 * Represents a party, participating in a contest.
 *
 * Field	            Description
 * contestId	        Integer. Id of the contest, in which party is participating.
 * members	            List of Member objects. Members of the party.
 * participantType	    Enum: CONTESTANT, PRACTICE, VIRTUAL, MANAGER, OUT_OF_COMPETITION.
 * teamId	            Integer. Can be absent. If party is a team, then it is a unique team id. Otherwise, this field is absent.
 * teamName	            String. Localized. Can be absent. If party is a team or ghost, then it is a localized name of the team. Otherwise, it is absent.
 * ghost	            Boolean. If true then this party is a ghost. It participated in the contest, but not on Codeforces. For example, Andrew Stankevich Contests in Gym has ghosts of the participants from Petrozavodsk Training Camp.
 * room	                Integer. Can be absent. Room of the party. If absent, then the party has no room.
 * startTimeSeconds	    Integer. Can be absent. Time, when this party started a contest.
 */
public class Party {

    private Integer contestId;

    private List<Member> members;

    private ParticipantType participantType;

    private Integer teamId;

    private String teamName;

    private Boolean ghost;

    private Integer room;

    private Integer startTimeSeconds;

    public Integer getContestId() {
        return contestId;
    }

    public void setContestId(Integer contestId) {
        this.contestId = contestId;
    }

    public List<Member> getMembers() {
        return members;
    }

    public void setMembers(List<Member> members) {
        this.members = members;
    }

    public ParticipantType getParticipantType() {
        return participantType;
    }

    public void setParticipantType(ParticipantType participantType) {
        this.participantType = participantType;
    }

    public Integer getTeamId() {
        return teamId;
    }

    public void setTeamId(Integer teamId) {
        this.teamId = teamId;
    }

    public String getTeamName() {
        return teamName;
    }

    public void setTeamName(String teamName) {
        this.teamName = teamName;
    }

    public Boolean getGhost() {
        return ghost;
    }

    public void setGhost(Boolean ghost) {
        this.ghost = ghost;
    }

    public Integer getRoom() {
        return room;
    }

    public void setRoom(Integer room) {
        this.room = room;
    }

    public Integer getStartTimeSeconds() {
        return startTimeSeconds;
    }

    public void setStartTimeSeconds(Integer startTimeSeconds) {
        this.startTimeSeconds = startTimeSeconds;
    }
}
