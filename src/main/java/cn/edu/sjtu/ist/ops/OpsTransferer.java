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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.sjtu.ist.ops.common.HadoopPath;
import cn.edu.sjtu.ist.ops.common.IndexReader;
import cn.edu.sjtu.ist.ops.common.IndexRecord;
import cn.edu.sjtu.ist.ops.common.OpsConf;
import cn.edu.sjtu.ist.ops.common.ShuffleHandlerTask;
import cn.edu.sjtu.ist.ops.common.ShuffleCompletedConf;
import cn.edu.sjtu.ist.ops.common.ShuffleConf;
import cn.edu.sjtu.ist.ops.util.OpsUtils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;;

class OpsTransferer extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(OpsTransferer.class);
    private final int num;
    private final OpsShuffleHandler shuffleHandler;
    private final OpsConf opsConf;

    public OpsTransferer(int num, OpsShuffleHandler shuffleHandler, OpsConf opsConf) {
        this.num = num;
        this.shuffleHandler = shuffleHandler;
        this.opsConf = opsConf;
    }

    @Override
    public void run() {
        this.setName("ops-transferer-[" + this.num + "]");
        try {
            logger.info("ops-transferer-[" + this.num + "] started");

            while (!Thread.currentThread().isInterrupted()) {
                ShuffleConf shuffle = null;
                shuffle = shuffleHandler.getPendingShuffle();
                transfer(shuffle);
            }
            // server.awaitTermination();
            // channel.wait();
        } catch (Exception e) {
            // TODO: handle exception
        }
    }

    private void transfer(ShuffleConf shuffle) throws IllegalArgumentException {

        logger.info("getPendingShuffle: task " + shuffle.getTask().getTaskId() + " to node "
                + shuffle.getDstNode().getIp());

        HashMap<String, IndexReader> irMap = this.shuffleHandler.getIndexReaderMap(shuffle.getTask().getJobId());
        IndexReader indexReader = irMap.get(shuffle.getTask().getTaskId());
        IndexRecord record = indexReader.getIndex(shuffle.getNum());
            
        ManagedChannel channel = ManagedChannelBuilder
        .forAddress(shuffle.getDstNode().getIp(), opsConf.getPortWorkerGRPC()).usePlaintext().build();
        OpsInternalGrpc.OpsInternalStub asyncStub = OpsInternalGrpc.newStub(channel);
        
        String path = OpsUtils.getMapOutputPath(shuffle.getTask().getJobId(), shuffle.getTask().getTaskId(),
        shuffle.getNum());
        StreamObserver<Chunk> requestObserver = asyncStub.transfer(new StreamObserver<ParentPath>() {
            String parentPath = "";
            
            @Override
            public void onNext(ParentPath path) {
                logger.debug("ParentPath: " + path.getPath());
                parentPath = path.getPath();
            }
            
            @Override
            public void onError(Throwable t) {
                logger.error("gRPC error.", t.getMessage());
                logger.info("gRPC channel break down. Re-addPendingShuffle.");
                shuffleHandler.addPendingShuffles(shuffle);
                try {
                    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                } catch (Exception e) {
                    e.printStackTrace();
                    //TODO: handle exception
                }
            }
            
            @Override
            public void onCompleted() {
                logger.debug("Transfer completed.");
                HadoopPath hadoopPath = new HadoopPath(new File(parentPath, path).toString(), record.getPartLength(),
                record.getRawLength());
                ShuffleCompletedConf shuffleC = new ShuffleCompletedConf(shuffle, hadoopPath);
                shuffleHandler.addPendingShuffleHandlerTask(new ShuffleHandlerTask(shuffleC));
                try {
                    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                } catch (Exception e) {
                    e.printStackTrace();
                    //TODO: handle exception
                }
            }
        });
            
        FileInputStream fileInput = null;
        BufferedInputStream input = null;
        try {
            long startOffset = record.getStartOffset();
            long partLength = record.getPartLength();

            logger.debug("Transfer indexRecord: " + record.toString());

            fileInput = new FileInputStream(new File(shuffle.getTask().getPath()));
            input = new BufferedInputStream(fileInput);
            input.skip(startOffset);

            int bufferSize = 1024 * 1024;// 1M

            byte[] buffer = new byte[bufferSize];
            int length;
            boolean isFirstChunk = true;
            while (true) {
                if (partLength < bufferSize) {
                    break;
                }
                length = input.read(buffer, 0, bufferSize);
                partLength -= length;
                if (length == -1) {
                    input.close();
                    throw new IllegalArgumentException("Unexpected file length.");
                }
                Chunk chunk = Chunk.newBuilder().setIsFirstChunk(isFirstChunk).setPath(path)
                        .setContent(ByteString.copyFrom(buffer, 0, length)).build();
                requestObserver.onNext(chunk);
                isFirstChunk = false;
            }
            length = input.read(buffer, 0, (int) partLength);
            Chunk chunk = Chunk.newBuilder().setIsFirstChunk(isFirstChunk).setPath(path)
                    .setContent(ByteString.copyFrom(buffer, 0, length)).build();
            requestObserver.onNext(chunk);
            requestObserver.onCompleted();

        } catch (RuntimeException e) {
            // Cancel RPC
            e.printStackTrace();
            requestObserver.onError(e);
            throw e;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            requestObserver.onError(e);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (fileInput != null) {
                    fileInput.close();
                }
                if (input != null) {
                    input.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
                //TODO: handle exception
            }
        }
    }
}