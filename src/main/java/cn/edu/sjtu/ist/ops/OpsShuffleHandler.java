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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.io.ByteSink;
import com.google.common.io.FileWriteMode;
import com.google.common.io.Files;
import com.google.gson.Gson;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.sjtu.ist.ops.common.JobConf;
import cn.edu.sjtu.ist.ops.common.MapConf;
import cn.edu.sjtu.ist.ops.common.OpsConf;
import cn.edu.sjtu.ist.ops.common.OpsNode;
import cn.edu.sjtu.ist.ops.common.CollectionConf;
import cn.edu.sjtu.ist.ops.common.IndexReader;
import cn.edu.sjtu.ist.ops.common.IndexRecord;
import cn.edu.sjtu.ist.ops.common.ShuffleCompletedConf;
import cn.edu.sjtu.ist.ops.common.ShuffleHandlerTask;
import cn.edu.sjtu.ist.ops.common.ShuffleConf;
import cn.edu.sjtu.ist.ops.common.MapTaskAlloc;
import cn.edu.sjtu.ist.ops.common.ReduceTaskAlloc;
import cn.edu.sjtu.ist.ops.util.EtcdService;
import cn.edu.sjtu.ist.ops.util.OpsUtils;
import cn.edu.sjtu.ist.ops.util.OpsWatcher;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class OpsShuffleHandler extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(OpsShuffleHandler.class);
    private final OpsNode host;
    private final Server workerServer;
    private final OpsConf opsConf;
    private final OpsWatcher jobWatcher;
    private final OpsWatcher mapCompletedWatcher;
    private final OpsWatcher mapTaskAllocWatcher;
    private final OpsWatcher reduceTaskAllocWatcher;
    private volatile boolean stopped = false;
    private final Set<ShuffleConf> pendingShuffles = new HashSet<>();
    private final Set<ShuffleHandlerTask> pendingShuffleHandlerTasks = new HashSet<>();
    private final HashMap<String, JobConf> jobs = new HashMap<>();
    /** Maps from a job to the mapTaskAlloc */
    private final HashMap<String, MapTaskAlloc> mapTaskAllocMapping = new HashMap<String, MapTaskAlloc>();
    /** Maps from a job to the reduceTaskAlloc */
    private final HashMap<String, ReduceTaskAlloc> reduceTaskAllocMapping = new HashMap<String, ReduceTaskAlloc>();
    private final HashMap<String, List<MapConf>> completedMapsMapping = new HashMap<String, List<MapConf>>();
    private final HashMap<String, HashMap<String, IndexReader>> indexReaderMapping = new HashMap<>();
    private final Random random = new Random();
    private Gson gson = new Gson();

    private final ManagedChannel masterChannel;
    private final OpsInternalGrpc.OpsInternalStub masterStub;

    public OpsShuffleHandler(OpsConf opsConf, OpsNode host) {
        EtcdService.initClient();

        this.opsConf = opsConf;
        this.host = host;
        OpsUtils.initLocalDir(this.opsConf.getDir());
        this.jobWatcher = new OpsWatcher(this, OpsUtils.ETCD_JOBS_PATH);
        this.mapCompletedWatcher = new OpsWatcher(this, OpsUtils.ETCD_MAPCOMPLETED_PATH,
                "/mapCompleted-" + host.getIp() + "-");
        this.mapTaskAllocWatcher = new OpsWatcher(this, OpsUtils.ETCD_MAPTASKALLOC_PATH);
        this.reduceTaskAllocWatcher = new OpsWatcher(this, OpsUtils.ETCD_REDUCETASKALLOC_PATH);

        this.masterChannel = ManagedChannelBuilder.forAddress(opsConf.getMaster().getIp(), opsConf.getPortMasterGRPC())
                .usePlaintext().build();
        this.masterStub = OpsInternalGrpc.newStub(masterChannel);

        this.workerServer = ServerBuilder.forPort(this.opsConf.getPortWorkerGRPC()).addService(new OpsInternalService())
                .build();
    }

    public void shutdown() throws InterruptedException {
        masterChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    @Override
    public void run() {
        this.setName("ops-shuffle-handler");
        try {
            this.workerServer.start();
            logger.info("gRPC workerServer started, listening on " + this.opsConf.getPortWorkerGRPC());
            logger.info("gRPC hadoopServer started, listening on " + this.opsConf.getPortHadoopGRPC());
            this.jobWatcher.start();
            this.mapCompletedWatcher.start();
            this.mapTaskAllocWatcher.start();
            this.reduceTaskAllocWatcher.start();

            while (!stopped && !Thread.currentThread().isInterrupted()) {
                ShuffleHandlerTask shuffleHandlerTask = null;
                shuffleHandlerTask = this.getPendingShuffleHandlerTask();
                switch (shuffleHandlerTask.getType()) {
                case SHUFFLECOMPLETED:
                    this.shuffleCompleted(shuffleHandlerTask.getShuffleC());
                    break;
                case COLLECTION:
                    this.collectIndexRecords(shuffleHandlerTask.getCollection());
                    break;
                default:
                    break;
                }
            }
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }

    public synchronized void watcherPut(String key, String value) {
        if (key == OpsUtils.ETCD_JOBS_PATH) {
            JobConf job = gson.fromJson(value, JobConf.class);
            this.jobs.put(job.getJobId(), job);
            this.indexReaderMapping.put(job.getJobId(), new HashMap<>());
            logger.info("Add new job: " + job.getJobId());

        } else if(key == OpsUtils.ETCD_MAPTASKALLOC_PATH) {
            MapTaskAlloc mapTaskAlloc = gson.fromJson(value, MapTaskAlloc.class);
            this.mapTaskAllocMapping.put(mapTaskAlloc.getJob().getJobId(), mapTaskAlloc);
            logger.info("Add MapTaskAlloc: " + mapTaskAlloc.toString());

        } else if(key == OpsUtils.ETCD_REDUCETASKALLOC_PATH) {
            ReduceTaskAlloc reduceTaskAlloc = gson.fromJson(value, ReduceTaskAlloc.class);
            this.reduceTaskAllocMapping.put(reduceTaskAlloc.getJob().getJobId(), reduceTaskAlloc);
            logger.info("Add ReduceTaskAlloc: " + reduceTaskAlloc.toString());

            String jobId = reduceTaskAlloc.getJob().getJobId();
            if(this.completedMapsMapping.containsKey(jobId)) {
                // If there are pendingCompletedMaps, start pre-shuffle.
                List<MapConf> completedMapList = this.completedMapsMapping.get(jobId);
                for (MapConf map : completedMapList) {
                    for (OpsNode node : reduceTaskAlloc.getJob().getWorkers()) {
                        for (Integer num : reduceTaskAlloc.getReducePreAllocOrder(node.getIp())) {
                            ShuffleConf shuffle = new ShuffleConf(map, node, num);
                            addPendingShuffles(shuffle);
                        }
                    }
                }
                this.completedMapsMapping.remove(jobId);
            }

        } else if (key == OpsUtils.ETCD_MAPCOMPLETED_PATH) {
            MapConf map = gson.fromJson(value, MapConf.class);
            if (!jobs.containsKey(map.getJobId())) {
                logger.error("JobId not found: " + map.getJobId());
                return;
            }
            JobConf job = jobs.get(map.getJobId());
            // Get IndexReader
            try {
                IndexReader indexReader = new IndexReader(map.getIndexPath().toString());
                HashMap<String, IndexReader> irMap = this.indexReaderMapping.get(job.getJobId());
                irMap.put(map.getTaskId(), indexReader);
            } catch (Exception e) {
                e.printStackTrace();
            }

            if(this.reduceTaskAllocMapping.containsKey(map.getJobId())) {
                // Add pendingShuffles, notify transferer to shuffle data
                ReduceTaskAlloc reduceTaskAlloc = this.reduceTaskAllocMapping.get(map.getJobId());
                for (OpsNode node : job.getWorkers()) {
                    for (Integer num : reduceTaskAlloc.getReducePreAllocOrder(node.getIp())) {
                        ShuffleConf shuffle = new ShuffleConf(map, node, num);
                        addPendingShuffles(shuffle);
                    }
                }
            } else {
                // If ReducePreAlloc is not ready, wait for it.
                if(!this.completedMapsMapping.containsKey(map.getJobId())) {
                    List<MapConf> mapList = new LinkedList<>();
                    mapList.add(map);
                    this.completedMapsMapping.put(map.getJobId(), mapList);
                } else {
                    List<MapConf> mapList = this.completedMapsMapping.get(map.getJobId());
                    mapList.add(map);
                    this.completedMapsMapping.put(map.getJobId(), mapList);
                }
                logger.debug("Waiting for ReduceTaskAlloc.");
            }
            

            // Add pendingShuffleHandlerTask, collection IndexRecord and put ETCD
            HashMap<String, IndexReader> irMap = this.indexReaderMapping.get(job.getJobId());
            IndexReader indexReader = irMap.get(map.getTaskId());
            if (indexReader == null) {
                logger.error("indexReader not found. mapTaskId -> " + map.getTaskId());
                return;
            }
            List<IndexRecord> records = new LinkedList<>();
            for(int i = 0; i < indexReader.getPartitions(); i++) {
                records.add(indexReader.getIndex(i));
            }
            CollectionConf collectionConf = new CollectionConf(map.getOpsNode().getIp(), map.getJobId(), map.getTaskId(), records);
            addPendingShuffleHandlerTask(new ShuffleHandlerTask(collectionConf));
        }
    }

    public synchronized ShuffleConf getPendingShuffle() throws InterruptedException {
        while (pendingShuffles.isEmpty()) {
            wait();
        }

        ShuffleConf shuffle = null;
        Iterator<ShuffleConf> iter = pendingShuffles.iterator();
        int numToPick = random.nextInt(pendingShuffles.size());
        for (int i = 0; i <= numToPick; ++i) {
            shuffle = iter.next();
        }

        pendingShuffles.remove(shuffle);

        logger.debug("Get pendingShuffle " + shuffle.toString());
        return shuffle;
    }

    public synchronized ShuffleHandlerTask getPendingShuffleHandlerTask() throws InterruptedException {
        while (this.pendingShuffleHandlerTasks.isEmpty()) {
            wait();
        }

        ShuffleHandlerTask task = null;

        Iterator<ShuffleHandlerTask> iter = this.pendingShuffleHandlerTasks.iterator();
        int numToPick = random.nextInt(this.pendingShuffleHandlerTasks.size());
        for (int i = 0; i <= numToPick; ++i) {
            task = iter.next();
        }
        this.pendingShuffleHandlerTasks.remove(task);

        logger.debug("Get pendingShuffleHandlerTask: " + task.toString());
        return task;
    }

    public JobConf getJob(String jobId) {
        return this.jobs.get(jobId);
    }

    public synchronized void addPendingShuffles(ShuffleConf shuffle) {
        pendingShuffles.add(shuffle);
        logger.debug("Add pendingShuffles task " + shuffle.getTask().getTaskId() + " to node "
                + shuffle.getDstNode().getIp());
        notifyAll();
    }

    public synchronized void addPendingShuffleHandlerTask(ShuffleHandlerTask task) {
        pendingShuffleHandlerTasks.add(task);
        logger.debug("Add pendingShuffleHandlerTasks: " + task.toString());
        notifyAll();
    }

    public void shuffleCompleted(ShuffleCompletedConf shuffleC) {
        EtcdService.put(
                OpsUtils.buildKeyShuffleCompleted(shuffleC.getDstNode().getIp(), shuffleC.getTask().getJobId(),
                        shuffleC.getNum().toString(), shuffleC.getTask().getTaskId()),
                gson.toJson(shuffleC.getHadoopPath()));
    }

    public void collectIndexRecords(CollectionConf collection) {
        EtcdService.put(
                OpsUtils.buildKeyIndexRecords(collection.getHost(), collection.getJobId(), collection.getMapId()),
                gson.toJson(collection));
    }

    public HashMap<String, IndexReader> getIndexReaderMap(String jobId) {
        return this.indexReaderMapping.get(jobId);
    }

    private class OpsInternalService extends OpsInternalGrpc.OpsInternalImplBase {
        @Override
        public StreamObserver<Chunk> transfer(StreamObserver<ParentPath> responseObserver) {
            return new StreamObserver<Chunk>() {
                @Override
                public void onNext(Chunk chunk) {
                    try {
                        boolean isFirstChunk = chunk.getIsFirstChunk();
                        String path = chunk.getPath();
                        File file = new File(opsConf.getDir(), path);
                        if (isFirstChunk) {
                            if (file.exists()) {
                                FileUtils.forceDelete(file);
                                logger.debug("Delete the namesake file: " + file.toString());
                            }
                            FileUtils.forceMkdirParent(file);
                            file.createNewFile();
                            logger.debug("mkdir & create file for shuffle data: " + file.toString());
                        }
                        ByteSink byteSink = Files.asByteSink(file, FileWriteMode.APPEND);
                        byteSink.write(chunk.getContent().toByteArray());
                        logger.debug("Receive chunk: {Path: " + file.toString() + ", Length: " + file.length() + "}");
                    } catch (IOException e){
                        e.printStackTrace();

                        logger.error("transfer error. Wait and retry.");
                        this.onError(e);
                        // try {
                        //     sleep(3000);
                        //     this.onNext(chunk);
                        // } catch (Exception ee) {
                        //     //TODO: handle exception
                        // }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void onError(Throwable t) {
                    logger.warn("Encountered error in exchange", t);
                    responseObserver.onError(t);
                }

                @Override
                public void onCompleted() {
                    ParentPath path = ParentPath.newBuilder().setPath(opsConf.getDir()).build();
                    responseObserver.onNext(path);
                    responseObserver.onCompleted();
                }
            };
        }
    }
}
