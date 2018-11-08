package cn.edu.sjtu.ist.ops.common;

public class TaskConf {
    private final String taskId;
    private final String jobId;
    private final OpsNode opsNode;

    public TaskConf(String taskId, String jobId, OpsNode node) {
        this.taskId = taskId;
        this.jobId = jobId;
        this.opsNode = node;
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