package cn.edu.sjtu.ist.ops.util;

import cn.edu.sjtu.ist.ops.util.EtcdService;

public class HeartbeatThread extends Thread {
    private String key;
    private String value;

    public HeartbeatThread(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public void run() {
        while (true) {
            EtcdService.register(this.key, this.value);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
