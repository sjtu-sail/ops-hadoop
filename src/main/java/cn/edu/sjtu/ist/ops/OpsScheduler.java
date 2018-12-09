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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.google.gson.Gson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.sjtu.ist.ops.common.JobConf;
import cn.edu.sjtu.ist.ops.common.MapConf;
import cn.edu.sjtu.ist.ops.common.OpsConf;
import cn.edu.sjtu.ist.ops.common.OpsNode;
import cn.edu.sjtu.ist.ops.common.OpsTask;
import cn.edu.sjtu.ist.ops.util.EtcdService;
import cn.edu.sjtu.ist.ops.util.OpsUtils;
import cn.edu.sjtu.ist.ops.util.OpsWatcher;
import cn.edu.sjtu.ist.ops.util.WatcherThread;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class OpsScheduler extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(OpsScheduler.class);

    private final Server server;
    private final Random random = new Random();
    private Map<String, ManagedChannel> workerChannels;
    private Map<String, OpsInternalGrpc.OpsInternalStub> workerStubs;
    private OpsWatcher jobWatcher;
    private OpsConf opsConf;
    private WatcherThread watcherThread;
    private HashMap<String, JobConf> jobs;
    private volatile boolean stopped;
    private Set<OpsTask> pendingOpsTasks;
    private Gson gson = new Gson();

    public OpsScheduler(OpsConf conf, WatcherThread watcher) {
        this.jobWatcher = new OpsWatcher(this, OpsUtils.ETCD_JOBS_PATH);
        this.opsConf = conf;
        this.watcherThread = watcher;
        this.stopped = false;
        this.jobs = new HashMap<>();
        this.pendingOpsTasks = new HashSet<>();
        this.workerChannels = new HashMap<>();
        this.workerStubs = new HashMap<>();

        setupWorkersGRPC();

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
                OpsTask opsTask = null;
                opsTask = this.getPendingOpsTask();
                switch (opsTask.getType()) {
                case ONSHUFFLE:
                    onShuffle(opsTask.getPendingTask());
                    break;
                case DISTRIBUTEJOB:
                    onDistributeJob(opsTask.getPendingJob());
                    break;
                default:
                    break;
                }
            }

        } catch (InterruptedException e) {
            // TODO: handle exception
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (this.server != null) {
                this.server.shutdown();
            }
        }
    }

    public synchronized void watcherPut(String key, String value) {
        if (key == OpsUtils.ETCD_JOBS_PATH) {
            JobConf job = gson.fromJson(value, JobConf.class);
            this.jobs.put(job.getJobId(), job);
            logger.info("Get new job: " + job.getJobId());
        }
    }

    public synchronized OpsTask getPendingOpsTask() throws InterruptedException {
        while (pendingOpsTasks.isEmpty()) {
            wait();
        }

        OpsTask opsTask = null;
        Iterator<OpsTask> iter = pendingOpsTasks.iterator();
        int numToPick = random.nextInt(pendingOpsTasks.size());
        for (int i = 0; i <= numToPick; ++i) {
            opsTask = iter.next();
        }

        pendingOpsTasks.remove(opsTask);

        logger.debug("Assigning " + opsTask.toString());
        return opsTask;
    }

    public synchronized void setupWorkersGRPC() {
        for (OpsNode worker : this.watcherThread.getWorkers()) {
            if (!this.workerChannels.containsKey(worker.getIp()) || !this.workerStubs.containsKey(worker.getIp())) {
                ManagedChannel channel = ManagedChannelBuilder.forAddress(worker.getIp(), opsConf.getPortWorkerGRPC())
                        .usePlaintext().build();
                OpsInternalGrpc.OpsInternalStub asyncStub = OpsInternalGrpc.newStub(channel);
                this.workerChannels.put(worker.getIp(), channel);
                this.workerStubs.put(worker.getIp(), asyncStub);
                logger.debug("Setup gRPC:" + worker.getIp() + ", " + opsConf.getPortWorkerGRPC());
            }
        }
    }

    public void onShuffle(MapConf task) {
        if (!this.watcherThread.getWorkers().contains(task.getOpsNode())) {
            logger.error("Worker not found: " + task.getOpsNode());
            return;
        }
        if (!this.workerStubs.containsKey(task.getOpsNode().getIp())) {
            setupWorkersGRPC();
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
            ShuffleMessage message = ShuffleMessage.newBuilder().setMapConf(gson.toJson(task)).build();
            requestObserver.onNext(message);
        } catch (RuntimeException e) {
            // Cancel RPC
            requestObserver.onError(e);
            throw e;
        }
        // Mark the end of requests
        requestObserver.onCompleted();
    }

    public void onDistributeJob(JobConf job) {
        Gson gson = new Gson();
        EtcdService.put(OpsUtils.ETCD_JOBS_PATH + "/job-" + job.getJobId() + "-JobConf", gson.toJson(job));
    }

    public synchronized void addPendingOpsTask(OpsTask opsTask) {
        switch (opsTask.getType()) {
        case ONSHUFFLE:
            MapConf task = opsTask.getPendingTask();
            // job.mapTaskCompleted(task);
            this.pendingOpsTasks.add(opsTask);
            notifyAll();
            break;
        case DISTRIBUTEJOB:
            this.pendingOpsTasks.add(opsTask);
            notifyAll();
            break;
        default:
            break;
        }
        logger.debug("Add pending OpsTask: " + opsTask.toString());
    }

    private class OpsInternalService extends OpsInternalGrpc.OpsInternalImplBase {
        @Override
        public StreamObserver<JobMessage> registerJob(StreamObserver<JobMessage> responseObserver) {
            return new StreamObserver<JobMessage>() {
                @Override
                public void onNext(JobMessage request) {
                    Gson gson = new Gson();
                    JobConf job = gson.fromJson(request.getJobConf(), JobConf.class);
                    addPendingOpsTask(new OpsTask(job));

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
        public StreamObserver<MapMessage> onMapComplete(StreamObserver<MapMessage> responseObserver) {
            return new StreamObserver<MapMessage>() {
                @Override
                public void onNext(MapMessage request) {
                    responseObserver.onNext(MapMessage.newBuilder().setMapConf("Response taskComplete").build());
                    Gson gson = new Gson();
                    MapConf task = gson.fromJson(request.getMapConf(), MapConf.class);
                    addPendingOpsTask(new OpsTask(task));
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
