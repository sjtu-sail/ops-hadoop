/*
 * Copyright 2018 SJTU IST Lab
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.sjtu.ist.ops.util;

import cn.edu.sjtu.ist.ops.common.OpsNode;
import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.watch.WatchEvent;
import com.coreos.jetcd.watch.WatchResponse;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WatcherThread extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(WatcherThread.class);
    private static Gson gson = new Gson();
    private String key;
    private Map<Long, OpsNode> workers;

    public WatcherThread(String key) {
        this.key = key;
        this.workers = new HashMap<Long, OpsNode>();
        EtcdService.getKVs(this.key).stream().forEach(
                kv -> this.workers.put(kv.getLease(), gson.fromJson(kv.getValue().toStringUtf8(), OpsNode.class)));
        logger.debug(this.workers.toString());
    }

    public List<OpsNode> getWorkers() {
        return this.workers.values().stream().collect(Collectors.toList());
    }

    public void run() {
        this.setName("ops-watcher");
        Watcher watcher = EtcdService.watch(this.key);
        while (true) {
            try {
                WatchResponse response = watcher.listen();
                WatchEvent event = response.getEvents().get(0);
                switch (event.getEventType()) {
                case PUT:
                    this.workers.put(event.getKeyValue().getLease(),
                            gson.fromJson(event.getKeyValue().getValue().toStringUtf8(), OpsNode.class));
                    logger.debug(workers.toString());
                    break;
                case DELETE:
                    String[] tokens = event.getKeyValue().getKey().toStringUtf8().split("/");
                    this.workers.remove(Long.parseLong(tokens[tokens.length - 1]));
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
