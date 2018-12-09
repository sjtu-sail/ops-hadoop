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

import cn.edu.sjtu.ist.ops.OpsScheduler;
import cn.edu.sjtu.ist.ops.OpsShuffleHandler;
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

public class OpsWatcher extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(WatcherThread.class);
    private OpsScheduler scheduler = null;
    private OpsShuffleHandler shuffleHandler = null;
    private final String key;

    public OpsWatcher(OpsScheduler scheduler, String key) {
        this.scheduler = scheduler;
        this.key = key;
        this.setName("ops-SchedulerWatcher-" + this.key);
        logger.debug("OpsWatcher");
    }

    public OpsWatcher(OpsShuffleHandler shuffleHandler, String key) {
        this.shuffleHandler = shuffleHandler;
        this.key = key;
        this.setName("ops-ShuffleHandlerWatcher-" + this.key);
        logger.debug("OpsWatcher");
    }

    public void run() {
        Watcher watcher = EtcdService.watch(this.key);
        logger.debug("Watch: " + this.key);
        while (true) {
            try {
                WatchResponse response = watcher.listen();
                WatchEvent event = response.getEvents().get(0);
                switch (event.getEventType()) {
                case PUT:
                    logger.debug("put");
                    if (this.scheduler != null) {
                        this.scheduler.watcherPut(key, event.getKeyValue().getValue().toStringUtf8());
                    } else {
                        this.shuffleHandler.watcherPut(key, event.getKeyValue().getValue().toStringUtf8());
                    }
                    break;
                case DELETE:
                    if (this.scheduler != null) {

                    } else {

                    }
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
