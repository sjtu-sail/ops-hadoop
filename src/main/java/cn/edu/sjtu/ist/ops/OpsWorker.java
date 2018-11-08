package cn.edu.sjtu.ist.ops;

import java.net.InetAddress;

import com.google.gson.Gson;

import cn.edu.sjtu.ist.ops.common.OpsNode;
import cn.edu.sjtu.ist.ops.util.EtcdService;
import cn.edu.sjtu.ist.ops.util.HeartbeatThread;

public class OpsWorker {
    public static void main(String[] args) throws InterruptedException {
        EtcdService.initClient();
        try {
            InetAddress addr = InetAddress.getLocalHost();
            OpsNode worker = new OpsNode(addr.getHostAddress(), addr.getHostName());
            Gson gson = new Gson();
            HeartbeatThread thread = new HeartbeatThread("ops/nodes/worker/", gson.toJson(worker));
            thread.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
