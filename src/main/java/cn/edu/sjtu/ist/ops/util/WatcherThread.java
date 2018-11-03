package cn.edu.sjtu.ist.ops.util;

import java.util.List;

import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.watch.WatchResponse;

public class WatcherThread extends Thread {
    private String key;
    private List<String> workers;

    public WatcherThread(String key) {
        this.key = key;
        this.workers = EtcdService.getAll("ops/nodes/worker");
        System.out.println(this.workers);
    }

    public List<String> getWorkers() {
        return this.workers;
    }

    public void run() {
        Watcher watcher = EtcdService.watch(this.key);
        while (true) {
            try {
                Thread.sleep(5000);
                WatchResponse response = watcher.listen();
                System.out.println(response);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
