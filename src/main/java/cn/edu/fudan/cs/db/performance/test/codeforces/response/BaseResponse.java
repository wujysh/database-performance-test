package cn.edu.fudan.cs.db.performance.test.codeforces.response;

import java.util.List;

/**
 * Created by wujy on 16-1-16.
 */
public class BaseResponse<T> {

    private String status;

    private List<T> result;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public List<T> getResult() {
        return result;
    }

    public void setResult(List<T> result) {
        this.result = result;
    }
}
