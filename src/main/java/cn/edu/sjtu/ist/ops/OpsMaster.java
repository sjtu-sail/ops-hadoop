package cn.edu.sjtu.ist.ops;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;

import com.google.gson.Gson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.sjtu.ist.ops.common.OpsConf;
import cn.edu.sjtu.ist.ops.common.OpsNode;
import cn.edu.sjtu.ist.ops.util.EtcdService;
import cn.edu.sjtu.ist.ops.util.HeartbeatThread;
import cn.edu.sjtu.ist.ops.util.WatcherThread;

public class OpsMaster extends OpsNode {

    private static final Logger logger = LoggerFactory.getLogger(OpsMaster.class);

    private OpsScheduler scheduler;
    private HeartbeatThread heartbeat;
    private WatcherThread watcher;

    public OpsMaster(String ip, String hostname) {
        super(ip, hostname);

        Gson gson = new Gson();
        this.heartbeat = new HeartbeatThread("ops/nodes/master/", gson.toJson(this));
        this.watcher = new WatcherThread("ops/nodes/worker");

        OpsConf opsConf = new OpsConf(this, this.watcher.getWorkers());
        this.scheduler = new OpsScheduler(opsConf, this.watcher);

    }

    public void start() throws UnknownHostException {
        this.heartbeat.start();
        this.watcher.start();

        logger.info("Master start");
        this.scheduler.start();
    }

    public void stop() {

    }

    private void blockUntilShutdown() throws InterruptedException {
        this.scheduler.join();
    }

    public static void main(String[] args) throws InterruptedException {
        EtcdService.initClient();

        try {
            InetAddress addr = InetAddress.getLocalHost();
            OpsMaster opsMaster = new OpsMaster(addr.getHostAddress(), addr.getHostName());

            opsMaster.start();
            opsMaster.blockUntilShutdown();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
