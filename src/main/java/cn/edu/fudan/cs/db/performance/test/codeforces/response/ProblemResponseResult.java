package cn.edu.fudan.cs.db.performance.test.codeforces.response;

import cn.edu.fudan.cs.db.performance.test.codeforces.entity.Problem;
import cn.edu.fudan.cs.db.performance.test.codeforces.entity.ProblemStatistics;

import java.util.List;

/**
 * Created by wujy on 16-1-16.
 */
public class ProblemResponseResult {

    private List<Problem> problems;

    private List<ProblemStatistics> problemStatistics;

    public List<Problem> getProblems() {
        return problems;
    }

    public void setProblems(List<Problem> problems) {
        this.problems = problems;
    }

    public List<ProblemStatistics> getProblemStatistics() {
        return problemStatistics;
    }

    public void setProblemStatistics(List<ProblemStatistics> problemStatistics) {
        this.problemStatistics = problemStatistics;
    }

}
