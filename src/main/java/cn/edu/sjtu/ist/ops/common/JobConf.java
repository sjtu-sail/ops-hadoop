package cn.edu.sjtu.ist.ops.common;

public class JobConf {
    private final String jobId;
    private final int numMap;
    private final int numReduce;

    public JobConf(String id, int map, int reduce) {
        this.jobId = id;
        this.numMap = map;
        this.numReduce = reduce;
    }

    public String getJobId() {
        return this.jobId;
    }

    public int getNumMap() {
        return this.numMap;
    }

    public int getNumReduce() {
        return this.numReduce;
    }
     
}