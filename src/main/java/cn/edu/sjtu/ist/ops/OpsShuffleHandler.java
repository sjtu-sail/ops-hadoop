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

import cn.edu.sjtu.ist.ops.common.OpsConf;
import cn.edu.sjtu.ist.ops.common.TaskConf;
import com.google.gson.Gson;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class OpsShuffleHandler extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(OpsShuffleHandler.class);
    private volatile boolean stopped;
    private final OpsConf opsConf;

    private final ManagedChannel channel;
    private final OpsInternalGrpc.OpsInternalStub asyncStub;

    public OpsShuffleHandler(OpsConf opsConf) {
        stopped = false;
        this.opsConf = opsConf;

        this.channel = ManagedChannelBuilder.forAddress(opsConf.getMaster().getIp(), opsConf.getPortMasterGRPC())
                .usePlaintext().build();
        this.asyncStub = OpsInternalGrpc.newStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    @Override
    public void run() {
        try {

            channel.wait();

        } catch (Exception e) {
            // TODO: handle exception
        }
    }

    public void taskComplete(TaskConf task) {
        StreamObserver<TaskMessage> requestObserver = asyncStub.onTaskComplete(new StreamObserver<TaskMessage>() {
            @Override
            public void onNext(TaskMessage msg) {
                logger.debug("ShuffleHandler: " + msg.getMsg());
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
            TaskMessage message = TaskMessage.newBuilder().setMsg(gson.toJson(task)).build();
            requestObserver.onNext(message);
        } catch (RuntimeException e) {
            // Cancel RPC
            requestObserver.onError(e);
            throw e;
        }
        // Mark the end of requests
        requestObserver.onCompleted();
    }

    private class OpsInternalService extends OpsInternalGrpc.OpsInternalImplBase {
        @Override
        public StreamObserver<ShuffleMessage> onShuffle(StreamObserver<ShuffleMessage> responseObserver) {
            return new StreamObserver<ShuffleMessage>() {
                @Override
                public void onNext(ShuffleMessage request) {
                    // responseObserver.onNext(ShuffleMessage.newBuilder().setMsg("ShuffleMessage").build());
                    logger.debug("ShuffleHandler: " + request.getMsg());
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