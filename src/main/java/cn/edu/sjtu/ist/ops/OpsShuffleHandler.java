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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import cn.edu.sjtu.ist.ops.common.ShuffleCompletedConf;
import cn.edu.sjtu.ist.ops.common.ShuffleConf;
import cn.edu.sjtu.ist.ops.common.TaskPreAlloc;
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
    private volatile boolean stopped = false;
    private Set<ShuffleConf> pendingShuffles = new HashSet<>();
    private Set<ShuffleCompletedConf> pendingCompletedShuffles = new HashSet<>();
    private HashMap<String, JobConf> jobs = new HashMap<>();
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

            while (!stopped && !Thread.currentThread().isInterrupted()) {
                ShuffleCompletedConf shuffleC = null;
                shuffleC = this.getCompletedShuffle();
                this.shuffleCompleted(shuffleC);
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
            logger.info("Add new job: " + job.getJobId());

        } else if (key == OpsUtils.ETCD_MAPCOMPLETED_PATH) {
            MapConf map = gson.fromJson(value, MapConf.class);
            if (!jobs.containsKey(map.getJobId())) {
                logger.error("JobId not found: " + map.getJobId());
                return;
            }
            JobConf job = jobs.get(map.getJobId());
            TaskPreAlloc preAlloc = job.getReducePreAlloc();
            for (OpsNode node : preAlloc.getNodesMap().values()) {
                for (Integer num : preAlloc.getTaskOrder(node.getIp())) {
                    ShuffleConf shuffle = new ShuffleConf(map, node, num);
                    addPendingShuffles(shuffle);
                }
            }
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

    public synchronized ShuffleCompletedConf getCompletedShuffle() throws InterruptedException {
        while (this.pendingCompletedShuffles.isEmpty()) {
            wait();
        }

        ShuffleCompletedConf task = null;

        Iterator<ShuffleCompletedConf> iter = this.pendingCompletedShuffles.iterator();
        int numToPick = random.nextInt(this.pendingCompletedShuffles.size());
        for (int i = 0; i <= numToPick; ++i) {
            task = iter.next();
        }
        this.pendingCompletedShuffles.remove(task);

        logger.debug("Get pendingCompletedShuffle " + task.toString());
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

    public synchronized void addPendingCompletedShuffle(ShuffleCompletedConf shuffleC) {
        pendingCompletedShuffles.add(shuffleC);
        logger.debug("Add pendingCompletedShuffle: " + shuffleC.toString());
        notifyAll();
    }

    public void shuffleCompleted(ShuffleCompletedConf shuffleC) {
        EtcdService.put(
                OpsUtils.buildKeyShuffleCompleted(shuffleC.getDstNode().getIp(), shuffleC.getTask().getJobId(),
                        shuffleC.getNum().toString(), shuffleC.getTask().getTaskId()),
                gson.toJson(shuffleC.getHadoopPath()));
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
                    } catch (Exception e) {
                        e.printStackTrace();
                        // TODO: handle exception
                    }
                }

                @Override
                public void onError(Throwable t) {
                    logger.warn("Encountered error in exchange", t);
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
