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

import java.util.List;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.google.gson.Gson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.sjtu.ist.ops.common.CollectionConf;
import cn.edu.sjtu.ist.ops.common.JobConf;
import cn.edu.sjtu.ist.ops.common.MapConf;
import cn.edu.sjtu.ist.ops.common.MapReport;
import cn.edu.sjtu.ist.ops.common.OpsConf;
import cn.edu.sjtu.ist.ops.common.OpsNode;
import cn.edu.sjtu.ist.ops.common.SchedulerTask;
import cn.edu.sjtu.ist.ops.common.SchedulerTask.SchedulerTaskType;
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

    public static final float DEFAULT_COMPLETED_MAPS_PERCENT_FOR_SCHEDULE_REDUCE = 0.1f;

    private final Random random = new Random();
    private Map<String, ManagedChannel> workerChannels = new HashMap<>();
    private Map<String, OpsInternalGrpc.OpsInternalStub> workerStubs = new HashMap<>();
    /** Maps from a job to the mapTaskAlloc */
    // private Map<String, MapTaskAlloc> mapTaskAllocMapping = new HashMap<String, MapTaskAlloc>();
    /** Maps from a job to the reduceTaskAlloc */
    private Map<String, ReduceTaskAlloc> reduceTaskAllocMapping = new HashMap<String, ReduceTaskAlloc>();
    /** Equals to Map<JobId, Map<ReduceId, Integer>> */
    private Map<String, Map<String, Integer>> reduceCounterMapping 
            = new HashMap<String, Map<String, Integer>>();
    /** Equals to Map<JobId, List<CollectionConf>> */
    private Map<String, List<CollectionConf>> collectionMapping 
            = new HashMap<String, List<CollectionConf>>();
    /** Equals to Map<JobId, Map<hostname, average time>> */
    private Map<String, Map<String, Report>> mapReportMapping
            = new HashMap<String, Map<String, Report>>();
    private Map<String, Boolean> isScheduleReduces = new HashMap<String, Boolean>();
    private Map<String, Boolean> isScheduleSecondMaps = new HashMap<String, Boolean>();
    private final OpsWatcher jobWatcher = new OpsWatcher(this, OpsUtils.ETCD_JOBS_PATH);
    private final OpsWatcher reduceWatcher = new OpsWatcher(this, OpsUtils.ETCD_REDUCETASKS_PATH);
    private final OpsWatcher indexRecordsWatcher = new OpsWatcher(this, OpsUtils.ETCD_INDEXRECORDS_PATH);
    private final OpsWatcher mapCompletedWatcher = new OpsWatcher(this, OpsUtils.ETCD_MAPCOMPLETED_PATH);
    private OpsConf opsConf;
    private WatcherThread watcherThread;
    private HashMap<String, JobConf> jobs = new HashMap<>();
    private volatile boolean stopped;
    private Set<SchedulerTask> pendingSchedulerTasks = new HashSet<>();
    private Gson gson = new Gson();

    public OpsScheduler(OpsConf conf, WatcherThread watcher) {
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
            this.indexRecordsWatcher.start();
            logger.info("gRPC Server started, listening on " + this.opsConf.getPortMasterGRPC());

            while (!stopped && !Thread.currentThread().isInterrupted()) {
                SchedulerTask schedulerTask = null;
                JobConf job = null;
                schedulerTask = this.getPendingSchedulerTask();
                switch (schedulerTask.getType()) {
                case SCHEDULE_MAP_FIRST:
                    job = schedulerTask.getPendingJob();
                    // MapTaskAlloc mapTaskAlloc = OpsScheStratagies.balancedMaps(job);
                    MapTaskAlloc mapTaskAlloc = OpsScheStratagies.firstPhaseMaps(job, 4);

                    logger.debug("Schedule first phase maps. Put MapTaskAllocMapping to etcd. Job: " 
                            + job.getJobId() + " MapTaskAlloc: " + gson.toJson(mapTaskAlloc));
                    // this.mapTaskAllocMapping.put(job.getJobId(), mapTaskAlloc);
                    EtcdService.put(OpsUtils.buildKeyMapTaskAlloc(job.getJobId()), gson.toJson(mapTaskAlloc));

                    break;
                case SCHEDULE_MAP_SECOND:
                    job = schedulerTask.getPendingJob();
                    mapTaskAlloc = OpsScheStratagies.secondPhaseMaps(job, 4);

                    logger.debug("Schedule second phase maps. Put MapTaskAllocMapping to etcd. Job: " 
                            + job.getJobId() + " MapTaskAlloc: " + gson.toJson(mapTaskAlloc));
                    // this.mapTaskAllocMapping.put(job.getJobId(), mapTaskAlloc);
                    EtcdService.put(OpsUtils.buildKeyMapTaskAlloc(job.getJobId()), gson.toJson(mapTaskAlloc));

                    break;
                case SCHEDULE_REDUCE:
                    job = schedulerTask.getPendingJob();
                    ReduceTaskAlloc reduceTaskAlloc = OpsScheStratagies.balancedReduces(job, schedulerTask.getCollectionList());

                    logger.debug("Put ReduceTaskAllocMapping & put etcd Job: " 
                            + job.getJobId() + " ReduceTaskAlloc: " + gson.toJson(reduceTaskAlloc));
                    this.reduceTaskAllocMapping.put(job.getJobId(), reduceTaskAlloc);
                    EtcdService.put(OpsUtils.buildKeyReduceTaskAlloc(job.getJobId()), gson.toJson(reduceTaskAlloc));

                    break;
                case REGISTER_REDUCE:
                    registerReduce(schedulerTask.getPendingReduce(), schedulerTask.getReduceNum());
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
            this.addPendingSchedulerTask(new SchedulerTask(job, SchedulerTaskType.SCHEDULE_MAP_FIRST));
            logger.info("Add new job: " + job.getJobId());
        } else if (key == OpsUtils.ETCD_REDUCETASKS_PATH) {
            ReduceConf reduce = gson.fromJson(value, ReduceConf.class);
            int reduceNum = this.distributeReduceNum(reduce.getJobId(), reduce.getOpsNode().getIp());
            this.addPendingSchedulerTask(new SchedulerTask(reduce, reduceNum));
            logger.info("Register new reduce task: " + reduce.toString());
        } else if (key == OpsUtils.ETCD_INDEXRECORDS_PATH) {
            CollectionConf collection = gson.fromJson(value, CollectionConf.class);
            String jobId = collection.getJobId();
            if(this.isScheduleReduces.containsKey(jobId)) {
                // logger.info("Already schedule reduce jobId: " + jobId);
                return;
            }
            if(!this.collectionMapping.containsKey(jobId)) {
                this.collectionMapping.put(jobId, new LinkedList<CollectionConf>());
            }
            List<CollectionConf> collectionList = this.collectionMapping.get(jobId);
            collectionList.add(collection);
            logger.info("Add new collection: " + collection.toString());

            // If collections(chunk size) meets default percent, start schedule reducePreAlloc.
            int completedMapsForScheduleReduce = (int)Math.ceil(
                    DEFAULT_COMPLETED_MAPS_PERCENT_FOR_SCHEDULE_REDUCE * 
                    this.jobs.get(jobId).getNumMap());
            if(collectionList.size() >= completedMapsForScheduleReduce) {
                this.isScheduleReduces.put(jobId, true);
                JobConf job = this.jobs.get(jobId);
                this.addPendingSchedulerTask(new SchedulerTask(job, collectionList));
                logger.info("Schedule Reduce. JobId: " + jobId + ", collectionListSize: " + collectionList.size());
            }
        } else if (key == OpsUtils.ETCD_MAPCOMPLETED_PATH) {
            MapReport map = gson.fromJson(value, MapReport.class);
            if (!jobs.containsKey(map.getJobId())) {
                logger.error("JobId not found: " + map.getJobId());
                return;
            }
            if (this.isScheduleSecondMaps.containsKey(map.getJobId())) {
                return;
            }
            // Collect map reports.
            JobConf job = jobs.get(map.getJobId());
            if (!this.mapReportMapping.containsKey(job.getJobId())) {
                this.mapReportMapping.put(job.getJobId(), new HashMap<String, Report>());
            }
            Map<String, Report> mapReports = this.mapReportMapping.get(job.getJobId());
            String host = map.getOpsNode().getIp();
            if (this.mapReportMapping.containsKey(host)) {
                Report old = mapReports.get(host);
                Report average = new Report((
                        old.finishTime + map.getMapFinishTime()) / 2, (old.dataSize + map.getOutputDataSize()) / 2);
                mapReports.put(host, average);
            } else {
                mapReports.put(host, new Report(map.getMapFinishTime(), map.getOutputDataSize()));
            }
            // If reach threshold, start second phase schedule.
            if (mapReports.keySet().size() >= job.getWorkers().size()) {
                this.isScheduleSecondMaps.put(job.getJobId(), true);
                this.addPendingSchedulerTask(new SchedulerTask(job, SchedulerTaskType.SCHEDULE_MAP_SECOND));
                logger.info("Job " + job.getJobId() + " start second phase maps schedule.");
            }
        }
    }

    public synchronized SchedulerTask getPendingSchedulerTask() throws InterruptedException {
        while (pendingSchedulerTasks.isEmpty()) {
            wait();
        }

        SchedulerTask schedulerTask = null;
        Iterator<SchedulerTask> iter = pendingSchedulerTasks.iterator();
        int numToPick = random.nextInt(pendingSchedulerTasks.size());
        for (int i = 0; i <= numToPick; ++i) {
            schedulerTask = iter.next();
        }

        pendingSchedulerTasks.remove(schedulerTask);

        logger.debug("Assigning " + schedulerTask.toString());
        return schedulerTask;
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

    public void registerReduce(ReduceConf reduce, Integer reduceNum) {
        logger.debug("registerReduce: "
                + OpsUtils.buildKeyReduceNum(reduce.getOpsNode().getIp(), reduce.getJobId(), reduce.getTaskId()));
        EtcdService.put(OpsUtils.buildKeyReduceNum(reduce.getOpsNode().getIp(), reduce.getJobId(), reduce.getTaskId()),
                reduceNum.toString());
    }

    public synchronized void addPendingSchedulerTask(SchedulerTask schedulerTask) {
        this.pendingSchedulerTasks.add(schedulerTask);
        notifyAll();
        logger.debug("Add pending SchedulerTask: " + schedulerTask.toString());
    }

    private static class Report {
        public Long finishTime;
        public Long dataSize;
        public Report(Long time, Long size) {
            this.finishTime = time;
            this.dataSize = size;
        }
    }
}
