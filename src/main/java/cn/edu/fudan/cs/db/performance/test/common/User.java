package cn.edu.fudan.cs.db.performance.test.common;

import java.util.List;

/**
 * Created by wujy on 16-1-16.
 */
public class User {

    private Integer id;

    private String username;

    private String degree;

    private List<Contest> contests;

    private List<Problem> triedProblems;

    private List<Problem> solvedProblems;

}
