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
import cn.edu.sjtu.ist.ops.common.OpsConf;
import cn.edu.sjtu.ist.ops.common.OpsNode;
import cn.edu.sjtu.ist.ops.common.OpsTask;
import cn.edu.sjtu.ist.ops.common.ReduceConf;
import cn.edu.sjtu.ist.ops.common.MapTaskAlloc;
import cn.edu.sjtu.ist.ops.common.ReduceTaskAlloc;
import cn.edu.sjtu.ist.ops.util.EtcdService;
import cn.edu.sjtu.ist.ops.util.OpsUtils;
import cn.edu.sjtu.ist.ops.util.OpsWatcher;
import cn.edu.sjtu.ist.ops.util.WatcherThread;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class OpsScheduler extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(OpsScheduler.class);

    private final Random random = new Random();
    private Map<String, ManagedChannel> workerChannels = new HashMap<>();
    private Map<String, OpsInternalGrpc.OpsInternalStub> workerStubs = new HashMap<>();
    /** Maps from a job to the mapTaskAlloc */
    private Map<String, MapTaskAlloc> mapTaskAllocMapping = new HashMap<String, MapTaskAlloc>();
    /** Maps from a job to the reduceTaskAlloc */
    private Map<String, ReduceTaskAlloc> reduceTaskAllocMapping = new HashMap<String, ReduceTaskAlloc>();
    private Map<String, Map<String, Integer>> reduceCounterMapping 
            = new HashMap<String, Map<String, Integer>>();
    private OpsWatcher jobWatcher;
    private OpsWatcher reduceWatcher;
    private OpsConf opsConf;
    private WatcherThread watcherThread;
    private HashMap<String, JobConf> jobs = new HashMap<>();
    private volatile boolean stopped;
    private Set<OpsTask> pendingOpsTasks = new HashSet<>();
    private Gson gson = new Gson();

    public OpsScheduler(OpsConf conf, WatcherThread watcher) {
        this.jobWatcher = new OpsWatcher(this, OpsUtils.ETCD_JOBS_PATH);
        this.reduceWatcher = new OpsWatcher(this, OpsUtils.ETCD_REDUCETASKS_PATH);
        this.opsConf = conf;
        this.watcherThread = watcher;
        this.stopped = false;

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
                case SCHEDULE:
                    scheduleJob(opsTask.getPendingJob());
                    break;
                case ONSHUFFLE:
                    // onShuffle(opsTask.getPendingMap());
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
            this.addPendingOpsTask(new OpsTask(job));
            logger.info("Add new job: " + job.getJobId());
        } else if (key == OpsUtils.ETCD_REDUCETASKS_PATH) {
            ReduceConf reduce = gson.fromJson(value, ReduceConf.class);
            int reduceNum = this.distributeReduceNum(reduce.getJobId(), reduce.getOpsNode().getIp());
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

    private int distributeReduceNum(String jobId, String host) {
        if(!reduceCounterMapping.containsKey(jobId)) {
            this.reduceCounterMapping.put(jobId, new HashMap<String, Integer>());
        }
        Map<String, Integer> reduceCounter = this.reduceCounterMapping.get(jobId);
        if(!reduceCounter.containsKey(host)) {
            reduceCounter.put(host, 0);
        }
        int count = reduceCounter.get(host) + 1;
        reduceCounter.put(host, count);
        int reduceNum = this.reduceTaskAllocMapping.get(jobId).getReducePreAllocOrder(host).get(count - 1);
        logger.debug("distributeReduceNum: Job: " + jobId + ", host: " + host 
                + ", count: " + count + ", reduceNum: " + reduceNum);
        return reduceNum;
    }

    public void scheduleJob(JobConf job) {
        logger.info("Schedule new job: " + job.getJobId());
        // TODO: Schedule jobs here.

        // naive strategy
        // mapTaskAlloc
        MapTaskAlloc mapTaskAlloc = new MapTaskAlloc(job);
        int mapPerNode = job.getNumMap() / job.getWorkers().size();
        int mapRemainder = job.getNumMap() % job.getWorkers().size();

        logger.debug("addMapPreAlloc remainder: [" + job.getWorkers().get(0).getIp() + ", " + mapRemainder + "]");
        String host = job.getWorkers().get(0).getIp();
        mapTaskAlloc.addMapPreAlloc(host, mapPerNode + mapRemainder);
        for (int i = 1; i < job.getWorkers().size(); i++) {
            OpsNode worker = job.getWorkers().get(i);
            logger.debug("addMapPreAlloc: [" + worker.getIp() + ", " + mapPerNode + "]");
            mapTaskAlloc.addMapPreAlloc(worker.getIp(), mapPerNode);
        }

        logger.debug("Put MapTaskAllocMapping and put etcd. Job: " 
                + job.getJobId() + " MapTaskAlloc: " + gson.toJson(mapTaskAlloc));
        this.mapTaskAllocMapping.put(job.getJobId(), mapTaskAlloc);

        EtcdService.put(OpsUtils.buildKeyMapTaskAlloc(job.getJobId()), gson.toJson(mapTaskAlloc));

        // reduce
        ReduceTaskAlloc reduceTaskAlloc = new ReduceTaskAlloc(job);
        int reducePerNode = job.getNumReduce() / job.getWorkers().size();
        int reduceRemainder = job.getNumReduce() % job.getWorkers().size();
        logger.debug("addReducePreAlloc remainder: [" + job.getWorkers().get(0).getIp() + ", " + reduceRemainder + "]");
        host = job.getWorkers().get(0).getIp();
        reduceTaskAlloc.addReducePreAlloc(host, reducePerNode + reduceRemainder);
        for (int i = 1; i < job.getWorkers().size(); i++) {
            OpsNode worker = job.getWorkers().get(i);
            logger.debug("addReducePreAlloc: [" + worker.getIp() + ", " + reducePerNode + "]");
            reduceTaskAlloc.addReducePreAlloc(worker.getIp(), reducePerNode);
        }

        logger.debug("Put ReduceTaskAllocMapping & put etcd Job: " 
                + job.getJobId() + " ReduceTaskAlloc: " + gson.toJson(reduceTaskAlloc));
        this.reduceTaskAllocMapping.put(job.getJobId(), reduceTaskAlloc);

        EtcdService.put(OpsUtils.buildKeyReduceTaskAlloc(job.getJobId()), gson.toJson(reduceTaskAlloc));
    }

    public void registerReduce(ReduceConf reduce, Integer reduceNum) {
        logger.debug("registerReduce: "
                + OpsUtils.buildKeyReduceNum(reduce.getOpsNode().getIp(), reduce.getJobId(), reduce.getTaskId()));
        EtcdService.put(OpsUtils.buildKeyReduceNum(reduce.getOpsNode().getIp(), reduce.getJobId(), reduce.getTaskId()),
                reduceNum.toString());
    }

    public synchronized void addPendingOpsTask(OpsTask opsTask) {
        switch (opsTask.getType()) {
        case SCHEDULE:
            this.pendingOpsTasks.add(opsTask);
            notifyAll();
            break;
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
