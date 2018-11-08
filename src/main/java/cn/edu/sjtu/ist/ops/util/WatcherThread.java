package cn.edu.sjtu.ist.ops.util;

import java.util.List;

import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.watch.WatchEvent;
import com.coreos.jetcd.watch.WatchResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WatcherThread extends Thread {
    private final static Logger logger = LoggerFactory.getLogger(WatcherThread.class.getName());

    private String key;
    private List<String> workers;

    public WatcherThread(String key) {
        this.key = key;
        this.workers = EtcdService.getAll(this.key);
        System.out.println(this.workers);
    }

    public List<String> getWorkers() {
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
                    String[] pTokens = event.getKeyValue().getKey().toStringUtf8().split("/");
                    this.workers.add(pTokens[pTokens.length - 1]);
                    logger.debug(workers.toString());
                    break;
                case DELETE:
                    String[] dTokens = event.getKeyValue().getKey().toStringUtf8().split("/");
                    this.workers.remove(dTokens[dTokens.length - 1]);
                    logger.debug(workers.toString());
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
