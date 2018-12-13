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

import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.watch.WatchEvent;
import com.coreos.jetcd.watch.WatchResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.sjtu.ist.ops.OpsScheduler;
import cn.edu.sjtu.ist.ops.OpsShuffleHandler;

import java.util.List;

public class OpsWatcher extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(WatcherThread.class);
    private OpsScheduler scheduler = null;
    private OpsShuffleHandler shuffleHandler = null;
    private final String key;
    private final String prefix;

    public OpsWatcher(OpsScheduler scheduler, String key) {
        this.scheduler = scheduler;
        this.key = key;
        this.prefix = null;
        this.setName("ops-SchedulerWatcher-" + this.key);
        logger.debug("ops-SchedulerWatcher-" + this.key + " start");
    }

    public OpsWatcher(OpsShuffleHandler shuffleHandler, String key) {
        this.shuffleHandler = shuffleHandler;
        this.key = key;
        this.prefix = null;
        this.setName("ops-ShuffleHandlerWatcher-" + this.key);
        logger.debug("ops-ShuffleHandlerWatcher-" + this.key + " start");
    }

    public OpsWatcher(OpsShuffleHandler shuffleHandler, String key, String prefix) {
        this.shuffleHandler = shuffleHandler;
        this.key = key;
        this.prefix = prefix;
        this.setName("ops-ShuffleHandlerWatcher-" + this.key + this.prefix);
        logger.debug("ops-ShuffleHandlerWatcher-" + this.key + this.prefix + " start");
    }

    public void run() {
        Watcher watcher;
        if (this.prefix == null) {
            watcher = EtcdService.watch(this.key);
            logger.debug("Watch: " + this.key);
        } else {
            watcher = EtcdService.watch(this.key + this.prefix);
            logger.debug("Watch: " + this.key + this.prefix);
        }

        while (true) {
            try {
                WatchResponse response = watcher.listen();
                List<WatchEvent> events = response.getEvents();

                events.forEach(event -> {
                    switch (event.getEventType()) {
                        case PUT:
                            logger.debug("put: " + event.getKeyValue().getKey().toStringUtf8());
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
                });
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
