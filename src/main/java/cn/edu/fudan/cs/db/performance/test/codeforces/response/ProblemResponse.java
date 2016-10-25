package cn.edu.fudan.cs.db.performance.test.codeforces.response;

/**
 * Created by wujy on 16-1-16.
 */
public class ProblemResponse {

    private String status;

    private ProblemResponseResult result;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public ProblemResponseResult getResult() {
        return result;
    }

    public void setResult(ProblemResponseResult result) {
        this.result = result;
    }

}