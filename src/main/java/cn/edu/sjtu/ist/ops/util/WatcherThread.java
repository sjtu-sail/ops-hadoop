package cn.edu.sjtu.ist.ops.util;

import java.util.List;
import java.util.stream.Collectors;

import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.watch.WatchEvent;
import com.coreos.jetcd.watch.WatchResponse;
import com.google.gson.Gson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.sjtu.ist.ops.common.OpsNode;

public class WatcherThread extends Thread {

    private final static Logger logger = LoggerFactory.getLogger(WatcherThread.class.getName());
    private static Gson gson = new Gson();
    private String key;
    private List<OpsNode> workers;

    public WatcherThread(String key) {
        this.key = key;
        this.workers = EtcdService.getValueList(this.key).stream().map(value -> gson.fromJson(value, OpsNode.class))
                .collect(Collectors.toList());
        System.out.println(this.workers);
    }

    public List<OpsNode> getWorkers() {
        return this.workers;
    }

    public void run() {
        Watcher watcher = EtcdService.watch(this.key);
        while (true) {
            try {
                WatchResponse response = watcher.listen();
                WatchEvent event = response.getEvents().get(0);
                switch (event.getEventType()) {
                case PUT:
                    this.workers.add(gson.fromJson(event.getKeyValue().getValue().toStringUtf8(), OpsNode.class));
                    System.out.println(workers.toString());
                    break;
                case DELETE:
                    // this.workers.remove(gson.fromJson(event.getKeyValue().getValue().toStringUtf8(),
                    // OpsNode.class));
                    System.out.println(event.getPrevKV().getValue());
                    break;
                case UNRECOGNIZED:
                    break;
                default:
                    break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
