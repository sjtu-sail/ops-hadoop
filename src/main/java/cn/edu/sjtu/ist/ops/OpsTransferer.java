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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.sjtu.ist.ops.common.JobConf;
import cn.edu.sjtu.ist.ops.common.OpsConf;
import cn.edu.sjtu.ist.ops.common.ShuffleConf;
import cn.edu.sjtu.ist.ops.common.TaskPreAlloc;
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
                doShuffle(shuffle);
            }
            // server.awaitTermination();
            // channel.wait();
        } catch (Exception e) {
            // TODO: handle exception
        }
    }

    private void doShuffle(ShuffleConf shuffle) {
        JobConf job = this.shuffleHandler.getJob(shuffle.getTask().getJobId());
        TaskPreAlloc preAlloc = job.getReducePreAlloc();

        logger.info("getPendingShuffle: task " + shuffle.getTask().getTaskId() + " to node "
                + shuffle.getDstNode().getIp());

        // TODO: shuffle file based on index file

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(shuffle.getDstNode().getIp(), opsConf.getPortWorkerGRPC()).usePlaintext().build();
        OpsInternalGrpc.OpsInternalStub asyncStub = OpsInternalGrpc.newStub(channel);
        StreamObserver<Chunk> requestObserver = asyncStub.transfer(new StreamObserver<StatusMessage>() {
            @Override
            public void onNext(StatusMessage msg) {

            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {
                channel.shutdown();
            }
        });

        try {
            BufferedInputStream input = new BufferedInputStream(new FileInputStream("file.test")); // for test
            int bufferSize = 256 * 1024;// 256k
            byte[] buffer = new byte[bufferSize];
            int length;
            while ((length = input.read(buffer, 0, bufferSize)) != -1) {
                Chunk chunk = Chunk.newBuilder().setContent(ByteString.copyFrom(buffer, 0, length)).build();
                requestObserver.onNext(chunk);
            }
        } catch (RuntimeException e) {
            // Cancel RPC
            requestObserver.onError(e);
            throw e;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            // TODO: Handle the exception
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Mark the end of requests
        requestObserver.onCompleted();
    }
}