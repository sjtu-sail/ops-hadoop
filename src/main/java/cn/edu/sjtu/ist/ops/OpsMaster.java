package cn.edu.sjtu.ist.ops;

import java.net.InetAddress;

import cn.edu.sjtu.ist.ops.util.EtcdService;
import cn.edu.sjtu.ist.ops.util.HeartbeatThread;
import cn.edu.sjtu.ist.ops.util.WatcherThread;

public class OpsMaster {
    public static void main(String[] args) throws InterruptedException {
        EtcdService.initClient();
        // EtcdService.put("ops/test", "test");
        try {
            // InetAddress addr = InetAddress.getLocalHost();
            // HeartbeatThread thread = new HeartbeatThread("ops/nodes/master/" +
            // addr.getHostAddress(),
            // addr.getHostAddress());
            WatcherThread thread = new WatcherThread("ops/nodes/worker/");
            thread.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
