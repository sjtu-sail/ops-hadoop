package cn.edu.sjtu.ist.ops;

import java.net.InetAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.sjtu.ist.ops.common.OpsNode;
import cn.edu.sjtu.ist.ops.util.EtcdService;
import cn.edu.sjtu.ist.ops.util.HeartbeatThread;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

public class OpsWorker {

    private static final Logger logger = LoggerFactory.getLogger(OpsWorker.class);
    private final ManagedChannel channel;
    private final OpsInternalGrpc.OpsInternalStub asyncStub;

    public OpsWorker(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
    }

    public OpsWorker(ManagedChannelBuilder<?> channelBuilder) {
        this.channel = channelBuilder.build();
        this.asyncStub = OpsInternalGrpc.newStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public CountDownLatch routeChat() {
        final CountDownLatch finishLatch = new CountDownLatch(1);
        StreamObserver<OpsRequest> requestObserver = asyncStub.exchange(new StreamObserver<OpsResponse>() {
            @Override
            public void onNext(OpsResponse response) {
                logger.debug("OpsWorker: " + response.getMsg());
            }

            @Override
            public void onError(Throwable t) {
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
                finishLatch.countDown();
            }
        });

        try {
            OpsRequest request = OpsRequest.newBuilder().setMsg("OpsWorker").build();
            requestObserver.onNext(request);
        } catch (RuntimeException e) {
            // Cancel RPC
            requestObserver.onError(e);
            throw e;
        }
        // Mark the end of requests
        requestObserver.onCompleted();

        // return the latch while receiving happens asynchronously
        return finishLatch;
    }

    public static void main(String[] args) throws InterruptedException {
        OpsWorker opsWorker = new OpsWorker("localhost", 14000);
        EtcdService.initClient();

        try {
            InetAddress addr = InetAddress.getLocalHost();
            OpsNode worker = new OpsNode(addr.getHostAddress(), addr.getHostName());
            Gson gson = new Gson();
            HeartbeatThread thread = new HeartbeatThread("ops/nodes/worker/", gson.toJson(worker));
            thread.start();

            CountDownLatch finishLatch = opsWorker.routeChat();

            if (!finishLatch.await(1, TimeUnit.MINUTES)) {
                logger.warn("exchange can not finish within 1 minutes");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            opsWorker.shutdown();
        }
    }
}
