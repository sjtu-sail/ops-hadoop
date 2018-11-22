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

package cn.edu.sjtu.ist.ops;

import cn.edu.sjtu.ist.ops.common.OpsConf;
import cn.edu.sjtu.ist.ops.common.OpsNode;
import cn.edu.sjtu.ist.ops.util.EtcdService;
import cn.edu.sjtu.ist.ops.util.HeartbeatThread;
import cn.edu.sjtu.ist.ops.util.WatcherThread;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class OpsMaster extends OpsNode {

    private static final Logger logger = LoggerFactory.getLogger(OpsMaster.class);

    private OpsScheduler scheduler;
    private HeartbeatThread heartbeat;
    private WatcherThread watcher;

    public OpsMaster(String ip, String hostname) {
        super(ip, hostname);

        Gson gson = new Gson();
        this.heartbeat = new HeartbeatThread("ops/nodes/master/", gson.toJson(this));
        this.watcher = new WatcherThread("ops/nodes/worker");

        OpsConf opsConf = new OpsConf(this, this.watcher.getWorkers());
        this.scheduler = new OpsScheduler(opsConf, this.watcher);

    }

    public void start() throws UnknownHostException {
        this.heartbeat.start();
        this.watcher.start();

        logger.info("Master start");
        this.scheduler.start();
    }

    public void stop() {

    }

    private void blockUntilShutdown() throws InterruptedException {
        this.scheduler.join();
    }

    public static void main(String[] args) throws InterruptedException {
        Thread.currentThread().setName("ops-master");
        EtcdService.initClient();

        try {
            InetAddress addr = InetAddress.getLocalHost();
            OpsMaster opsMaster = new OpsMaster(addr.getHostAddress(), addr.getHostName());

            opsMaster.start();
            opsMaster.blockUntilShutdown();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
