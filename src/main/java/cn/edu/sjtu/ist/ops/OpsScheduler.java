package cn.edu.sjtu.ist.ops;

import java.util.ArrayList;
import java.util.HashMap;

import com.google.gson.Gson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.sjtu.ist.ops.common.JobConf;
import cn.edu.sjtu.ist.ops.common.JobStatus;
import cn.edu.sjtu.ist.ops.common.OpsConf;
import cn.edu.sjtu.ist.ops.common.TaskConf;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class OpsScheduler extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(OpsScheduler.class);

    private final Server server;

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

        this.server = ServerBuilder.forPort(this.opsConf.getPortMasterGRPC()).addService(new OpsInternalService())
                .build();
    }

    @Override
    public void run() {
        try {
            this.server.start();
            logger.info("Server started, listening on " + this.opsConf.getPortMasterGRPC());

            server.awaitTermination();

        } catch (InterruptedException e) {
            // TODO: handle exception
        } catch (Exception e) {

        } finally {
            if (this.server != null) {
                this.server.shutdown();
            }
        }
    }

    public void shuffle() {
        for (int i = this.pendingMapsIndex; i < this.pendingMaps.size(); i++) {
            this.pendingMapsIndex++;

            logger.info("Do Shuffle");
            // TODO: Use gRPC to notify ShuffleHandler.
        }

    }

    public void registJob(JobConf job) {
        jobs.put(job.getJobId(), new JobStatus(job));

    }

    public void taskComplete(TaskConf task) {
        JobStatus job = jobs.get(task.getJobId());
        logger.info("Task " + task.getTaskId() + " completed");
        if (task.getIsMap()) {
            // job.mapTaskCompleted(task);
            this.pendingMaps.add(task);
        } else {
            // job.reduceCompleted(task);
        }
    }

    private class OpsInternalService extends OpsInternalGrpc.OpsInternalImplBase {
        @Override
        public StreamObserver<TaskMessage> onTaskComplete(StreamObserver<TaskMessage> responseObserver) {
            return new StreamObserver<TaskMessage>() {
                @Override
                public void onNext(TaskMessage request) {
                    responseObserver.onNext(TaskMessage.newBuilder().setMsg("Response taskComplete").build());
                    Gson gson = new Gson();
                    TaskConf task = gson.fromJson(request.getMsg(), TaskConf.class);
                    logger.info("OpsScheduler: " + task.toString());
                    taskComplete(task);
                }

                @Override
                public void onError(Throwable t) {
                    logger.warn("Encountered error in exchange", t);
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }
    }
}
