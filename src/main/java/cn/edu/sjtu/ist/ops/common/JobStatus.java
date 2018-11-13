package cn.edu.sjtu.ist.ops.common;

public class JobStatus {
    public static enum Phase{STARTING, MAP, SHUFFLE, REDUCE, CLEANUP}
    private final JobConf jobConf;
    private long startTime; //in ms
    private long finishTime;
    private volatile Phase phase = Phase.MAP; // set initial phase to MAP for now.

    public JobStatus(JobConf jobConf) {
        this.startTime = System.currentTimeMillis();
        this.jobConf = jobConf;
    }

    public void mapTaskCompleted(TaskConf task) {

    }

    /**
     * Get start time of the task. 
     * @return 0 is start time is not set, else returns start time. 
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Set startTime of the task.
     * @param startTime start time
     */
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    /**
     * Get task finish time.
     * @return finish time of the task. 
     */
    public long getFinishTime() {
        return finishTime;
    }

    /**
     * Sets finishTime for the task status.
     * @param finishTime finish time of task.
     */
    void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }
}