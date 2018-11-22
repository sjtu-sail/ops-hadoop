/*
 * Copyright 2018 SJTU IST Lab
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.sjtu.ist.ops;

import cn.edu.sjtu.ist.ops.common.JobConf;
import cn.edu.sjtu.ist.ops.common.OpsConf;
import cn.edu.sjtu.ist.ops.common.OpsNode;
import cn.edu.sjtu.ist.ops.common.TaskConf;
import cn.edu.sjtu.ist.ops.util.WatcherThread;
import com.google.gson.Gson;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class OpsScheduler extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(OpsScheduler.class);

    private final Server server;
    private final Random random = new Random();
    private Map<String, ManagedChannel> workerChannels;
    private Map<String, OpsInternalGrpc.OpsInternalStub> workerStubs;
    private OpsConf opsConf;
    private WatcherThread watcherThread;
    private HashMap<String, JobConf> jobs;
    private volatile boolean stopped;
    private Set<TaskConf> pendingTasks;

    public OpsScheduler(OpsConf conf, WatcherThread watcher) {
        this.opsConf = conf;
        this.watcherThread = watcher;
        this.stopped = false;
        this.jobs = new HashMap<>();
        this.pendingTasks = new HashSet<>();
        this.workerChannels = new HashMap<>();
        this.workerStubs = new HashMap<>();

        setupWorkersRPC();

        this.server = ServerBuilder.forPort(this.opsConf.getPortMasterGRPC()).addService(new OpsInternalService())
                .build();
    }

    @Override
    public void run() {
        this.setName("ops-scheduler");
        try {
            this.server.start();
            logger.info("gRPC Server started, listening on " + this.opsConf.getPortMasterGRPC());

            while (!stopped && !Thread.currentThread().isInterrupted()) {
                TaskConf task = null;
                task = this.getPendingTask();
                onShuffle(task);
            }
            // server.awaitTermination();
        } catch (InterruptedException e) {
            // TODO: handle exception
        } catch (Exception e) {

        } finally {
            if (this.server != null) {
                this.server.shutdown();
            }
        }
    }

    public synchronized TaskConf getPendingTask() throws InterruptedException {
        while (pendingTasks.isEmpty()) {
            wait();
        }

        TaskConf task = null;
        Iterator<TaskConf> iter = pendingTasks.iterator();
        int numToPick = random.nextInt(pendingTasks.size());
        for (int i = 0; i <= numToPick; ++i) {
            task = iter.next();
        }

        pendingTasks.remove(task);

        logger.debug("Assigning " + task.toString());
        return task;
    }

    public void setupWorkersRPC() {
        for (OpsNode worker : this.watcherThread.getWorkers()) {
            if (!this.workerChannels.containsKey(worker.getIp()) || !this.workerStubs.containsKey(worker.getIp())) {
                ManagedChannel channel = ManagedChannelBuilder
                        .forAddress(opsConf.getMaster().getIp(), opsConf.getPortMasterGRPC()).usePlaintext().build();
                OpsInternalGrpc.OpsInternalStub asyncStub = OpsInternalGrpc.newStub(channel);
                this.workerChannels.put(worker.getIp(), channel);
                this.workerStubs.put(worker.getIp(), asyncStub);
                logger.debug("Setup gRPC " + worker.toString());
            }
        }
    }

    public void onShuffle(TaskConf task) {
        if (!this.workerStubs.containsKey(task.getOpsNode().getIp())) {
            setupWorkersRPC();
        }
        if (!this.workerStubs.containsKey(task.getOpsNode().getIp())) {
            logger.error("Worker not found.");
            return;
        }

        StreamObserver<ShuffleMessage> requestObserver = this.workerStubs.get(task.getOpsNode().getIp())
                .onShuffle(new StreamObserver<ShuffleMessage>() {
                    @Override
                    public void onNext(ShuffleMessage msg) {
                        logger.debug("");
                    }

                    @Override
                    public void onError(Throwable t) {

                    }

                    @Override
                    public void onCompleted() {

                    }
                });
        try {
            Gson gson = new Gson();
            logger.debug("Shuffle task: " + task.toString());
            ShuffleMessage message = ShuffleMessage.newBuilder().setTaskConf(gson.toJson(task)).build();
            requestObserver.onNext(message);
        } catch (RuntimeException e) {
            // Cancel RPC
            requestObserver.onError(e);
            throw e;
        }
        // Mark the end of requests
        requestObserver.onCompleted();
    }

    public void distributeJob(JobConf job) {
        updateWorkers();
        jobs.put(job.getJobId(), job);

        for (OpsInternalGrpc.OpsInternalStub stub : this.workerStubs.values()) {
            StreamObserver<JobMessage> requestObserver = stub.distributeJob(new StreamObserver<JobMessage>() {
                @Override
                public void onNext(JobMessage msg) {
                    logger.debug("");
                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onCompleted() {

                }
            });
            try {
                Gson gson = new Gson();
                JobMessage message = JobMessage.newBuilder().setJobConf(gson.toJson(job)).build();
                requestObserver.onNext(message);
            } catch (RuntimeException e) {
                // Cancel RPC
                requestObserver.onError(e);
                throw e;
            }
            // Mark the end of requests
            requestObserver.onCompleted();
        }
    }

    private void updateWorkers() {
        this.opsConf.setWorkers(this.watcherThread.getWorkers());
    }

    public synchronized void taskComplete(TaskConf task) {
        JobConf job = jobs.get(task.getJobId());
        logger.debug("Task " + task.getTaskId() + " completed");
        if (task.getIsMap()) {
            // job.mapTaskCompleted(task);
            this.pendingTasks.add(task);
            notifyAll();
        } else {
            // job.reduceCompleted(task);
        }
    }

    private class OpsInternalService extends OpsInternalGrpc.OpsInternalImplBase {
        @Override
        public StreamObserver<JobMessage> registerJob(StreamObserver<JobMessage> responseObserver) {
            return new StreamObserver<JobMessage>() {
                @Override
                public void onNext(JobMessage request) {
                    Gson gson = new Gson();
                    logger.debug(request.getJobConf());
                    JobConf job = gson.fromJson(request.getJobConf(), JobConf.class);
                    jobs.put(job.getJobId(), job);
                    logger.info("Register Job: " + job.toString());
                    logger.debug("Pending Jobs: " + jobs.toString());
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

        @Override
        public StreamObserver<TaskMessage> onTaskComplete(StreamObserver<TaskMessage> responseObserver) {
            return new StreamObserver<TaskMessage>() {
                @Override
                public void onNext(TaskMessage request) {
                    responseObserver.onNext(TaskMessage.newBuilder().setTaskConf("Response taskComplete").build());
                    Gson gson = new Gson();
                    TaskConf task = gson.fromJson(request.getTaskConf(), TaskConf.class);
                    logger.debug("OpsScheduler: " + task.toString());
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
