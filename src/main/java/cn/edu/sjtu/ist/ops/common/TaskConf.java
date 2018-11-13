package cn.edu.sjtu.ist.ops.common;

import com.google.gson.Gson;

public class TaskConf {
    private final Boolean isMap;
    private final String taskId;
    private final String jobId;
    private final OpsNode opsNode;

    public TaskConf(Boolean isMap, String taskId, String jobId, OpsNode node) {
        this.isMap = isMap;
        this.taskId = taskId;
        this.jobId = jobId;
        this.opsNode = node;
    }
    
    @Override
    public String toString() {
        return new Gson().toJson(this);
    }

    public Boolean getIsMap() {
        return this.isMap;
    }

    public String getTaskId() {
        return this.taskId;
    }

    public String getJobId() {
        return this.jobId;
    }

    public OpsNode getOpsNode() {
        return this.opsNode;
    }
    
}