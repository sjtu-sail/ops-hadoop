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
import cn.edu.sjtu.ist.ops.common.ReduceConf;
import cn.edu.sjtu.ist.ops.util.EtcdService;
import cn.edu.sjtu.ist.ops.util.OpsUtils;
import cn.edu.sjtu.ist.ops.util.OpsWatcher;
import cn.edu.sjtu.ist.ops.util.WatcherThread;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class OpsScheduler extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(OpsScheduler.class);

    private final Random random = new Random();
    private Map<String, ManagedChannel> workerChannels;
    private Map<String, OpsInternalGrpc.OpsInternalStub> workerStubs;
    private OpsWatcher jobWatcher;
    private OpsWatcher reduceWatcher;
    private OpsConf opsConf;
    private WatcherThread watcherThread;
    private HashMap<String, JobConf> jobs;
    private volatile boolean stopped;
    private Set<OpsTask> pendingOpsTasks;
    private Gson gson = new Gson();

    public OpsScheduler(OpsConf conf, WatcherThread watcher) {
        this.jobWatcher = new OpsWatcher(this, OpsUtils.ETCD_JOBS_PATH);
        this.reduceWatcher = new OpsWatcher(this, OpsUtils.ETCD_REDUCETASKS_PATH);
        this.opsConf = conf;
        this.watcherThread = watcher;
        this.stopped = false;
        this.jobs = new HashMap<>();
        this.pendingOpsTasks = new HashSet<>();
        this.workerChannels = new HashMap<>();
        this.workerStubs = new HashMap<>();

        setupWorkersGRPC();

    }

    @Override
    public void run() {
        this.setName("ops-scheduler");
        try {
            this.jobWatcher.start();
            this.reduceWatcher.start();
            logger.info("gRPC Server started, listening on " + this.opsConf.getPortMasterGRPC());

            while (!stopped && !Thread.currentThread().isInterrupted()) {
                OpsTask opsTask = null;
                opsTask = this.getPendingOpsTask();
                switch (opsTask.getType()) {
                case ONSHUFFLE:
                    onShuffle(opsTask.getPendingMap());
                    break;
                case REGISTER_REDUCE:
                    registerReduce(opsTask.getPendingReduce(), opsTask.getReduceNum());
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

        }
    }

    public synchronized void watcherPut(String key, String value) {
        if (key == OpsUtils.ETCD_JOBS_PATH) {
            JobConf job = gson.fromJson(value, JobConf.class);
            this.jobs.put(job.getJobId(), job);
            logger.info("Add new job: " + job.getJobId());
        } else if (key == OpsUtils.ETCD_REDUCETASKS_PATH) {
            ReduceConf reduce = gson.fromJson(value, ReduceConf.class);
            JobConf job = this.jobs.get(reduce.getJobId());
            Integer reduceNum = job.distributeReduceNum(reduce.getOpsNode().getIp());
            this.jobs.put(reduce.getJobId(), job);
            this.addPendingOpsTask(new OpsTask(reduce, reduceNum));
            logger.info("Register new reduce task: " + reduce.toString());
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

    }

    public void registerReduce(ReduceConf reduce, Integer reduceNum) {
        logger.debug("registerReduce: "
                + OpsUtils.buildKeyReduceNum(reduce.getOpsNode().getIp(), reduce.getJobId(), reduce.getTaskId()));
        EtcdService.put(OpsUtils.buildKeyReduceNum(reduce.getOpsNode().getIp(), reduce.getJobId(), reduce.getTaskId()),
                reduceNum.toString());
    }

    public synchronized void addPendingOpsTask(OpsTask opsTask) {
        switch (opsTask.getType()) {
        case ONSHUFFLE:
            // MapConf task = opsTask.getPendingMap();
            // job.mapTaskCompleted(task);
            this.pendingOpsTasks.add(opsTask);
            notifyAll();
            break;
        case REGISTER_REDUCE:
            this.pendingOpsTasks.add(opsTask);
            notifyAll();
            break;
        default:
            break;
        }
        logger.debug("Add pending OpsTask: " + opsTask.toString());
    }
}
