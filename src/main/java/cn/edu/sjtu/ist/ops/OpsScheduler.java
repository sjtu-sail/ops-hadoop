package cn.edu.sjtu.ist.ops;

import java.util.ArrayList;
import java.util.HashMap;

import cn.edu.sjtu.ist.ops.common.JobConf;
import cn.edu.sjtu.ist.ops.common.JobStatus;
import cn.edu.sjtu.ist.ops.common.OpsConf;
import cn.edu.sjtu.ist.ops.common.TaskConf;

public class OpsScheduler extends Thread {
    private OpsConf opsConf;
    private HashMap<String, JobStatus> jobs;
    private volatile boolean stopped;
    private ArrayList<TaskConf> pendingMaps;
    private int pendingMapsIndex;

    public OpsScheduler(OpsConf conf) {
        this.opsConf = conf;
        this.stopped = false;
        this.jobs = new HashMap<>();
        this.pendingMaps = new ArrayList<>();
        this.pendingMapsIndex = 0;
    }

    @Override
    public void run() {
        try {
            while (!stopped && !Thread.currentThread().isInterrupted()) {
                wait();
            
                shuffle();
            }
        } catch (InterruptedException e) {
            // TODO: handle exception
        }
        
    }

    private void shuffle() {
        for(int i = this.pendingMapsIndex; i < this.pendingMaps.size(); i++) {
            this.pendingMapsIndex++;

            // TODO: Use gRPC to notify ShuffleHandler.
        }

    }

    public void registJob(JobConf job) {
        jobs.put(job.getJobId(), new JobStatus(job));

    }

    public void mapCompleted(TaskConf task) {
        JobStatus job = jobs.get(task.getJobId());
        job.mapCompleted(task);
        this.pendingMaps.add(task);

        notifyAll();
    }

    public void reduceCompleted(TaskConf task) {

    }
}
