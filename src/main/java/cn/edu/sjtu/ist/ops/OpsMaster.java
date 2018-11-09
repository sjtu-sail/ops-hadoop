package cn.edu.sjtu.ist.ops;

import java.io.IOException;
import java.net.InetAddress;

import com.google.gson.Gson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.sjtu.ist.ops.common.OpsNode;
import cn.edu.sjtu.ist.ops.util.EtcdService;
import cn.edu.sjtu.ist.ops.util.HeartbeatThread;
import cn.edu.sjtu.ist.ops.util.WatcherThread;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class OpsMaster {

    private static final Logger logger = LoggerFactory.getLogger(OpsMaster.class);
    private final int port;
    private final Server server;

    public OpsMaster(int port) {
        this.port = port;
        this.server = ServerBuilder.forPort(port).addService(new OpsInternalService()).build();
    }

    public void start() throws IOException {
        server.start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                OpsMaster.this.stop();
            }
        });
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        OpsMaster opsMaster = new OpsMaster(14000);
        EtcdService.initClient();

        try {
            InetAddress addr = InetAddress.getLocalHost();
            OpsNode master = new OpsNode(addr.getHostAddress(), addr.getHostName());
            Gson gson = new Gson();
            HeartbeatThread heartbeatThread = new HeartbeatThread("ops/nodes/master/", gson.toJson(master));
            WatcherThread watcherThread = new WatcherThread("ops/nodes/worker");
            heartbeatThread.start();
            watcherThread.start();

            opsMaster.start();
            opsMaster.blockUntilShutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class OpsInternalService extends OpsInternalGrpc.OpsInternalImplBase {
        @Override
        public StreamObserver<OpsRequest> exchange(StreamObserver<OpsResponse> responseObserver) {
            return new StreamObserver<OpsRequest>() {
                @Override
                public void onNext(OpsRequest request) {
                    responseObserver.onNext(OpsResponse.newBuilder().setMsg("OpsMaster").build());
                    logger.debug("OpsMaster: " + request.getMsg());
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
