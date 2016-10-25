package cn.edu.fudan.cs.db.performance.test.common;

import java.util.Date;
import java.util.List;

/**
 * Created by wujy on 16-1-16.
 */
public class Contest {

    private Integer id;

    private String title;

    private String description;

    private Long contestant;

    private Date startTime;

    private Date endTime;

    private List<Problem> problems;

    private List<User> users;

}
