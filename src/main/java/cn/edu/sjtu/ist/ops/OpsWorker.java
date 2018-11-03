package cn.edu.sjtu.ist.ops;

import java.net.InetAddress;

import cn.edu.sjtu.ist.ops.util.EtcdService;
import cn.edu.sjtu.ist.ops.util.HeartbeatThread;

public class OpsWorker {
    public static void main(String[] args) throws InterruptedException {
        EtcdService.initClient();
        try {
            InetAddress addr = InetAddress.getLocalHost();
            HeartbeatThread thread = new HeartbeatThread("ops/nodes/worker/" + addr.getHostAddress(),
                    addr.getHostAddress());
            thread.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
