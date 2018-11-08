package cn.edu.sjtu.ist.ops;

import java.net.InetAddress;

import com.google.gson.Gson;

import cn.edu.sjtu.ist.ops.common.OpsNode;
import cn.edu.sjtu.ist.ops.util.EtcdService;
import cn.edu.sjtu.ist.ops.util.HeartbeatThread;
import cn.edu.sjtu.ist.ops.util.WatcherThread;

public class OpsMaster {
    public static void main(String[] args) throws InterruptedException {
        EtcdService.initClient();
        // EtcdService.put("ops/test", "test");
        try {
            InetAddress addr = InetAddress.getLocalHost();
            OpsNode master = new OpsNode(addr.getHostAddress(), addr.getHostName());
            Gson gson = new Gson();
            HeartbeatThread heartbeatThread = new HeartbeatThread("ops/nodes/master/", gson.toJson(master));
            WatcherThread watcherThread = new WatcherThread("ops/nodes/worker");
            heartbeatThread.start();
            watcherThread.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
