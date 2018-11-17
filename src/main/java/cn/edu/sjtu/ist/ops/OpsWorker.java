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
import cn.edu.sjtu.ist.ops.common.TaskConf;
import cn.edu.sjtu.ist.ops.util.EtcdService;
import cn.edu.sjtu.ist.ops.util.HeartbeatThread;
import cn.edu.sjtu.ist.ops.util.WatcherThread;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

public class OpsWorker extends OpsNode {

    private static final Logger logger = LoggerFactory.getLogger(OpsWorker.class);
    private HeartbeatThread heartbeat;
    private WatcherThread watcher;
    private OpsShuffleHandler shuffleHandler;

    public OpsWorker(String ip, String hostname) {
        super(ip, hostname);

        Gson gson = new Gson();
        this.heartbeat = new HeartbeatThread("ops/nodes/worker/", gson.toJson(this));
        this.watcher = new WatcherThread("ops/nodes/worker");

        // TODO: get Master from etcd
        OpsNode master = new OpsNode("localhost", "localhost");
        OpsConf opsConf = new OpsConf(master, this.watcher.getWorkers());

        shuffleHandler = new OpsShuffleHandler(opsConf);
    }

    public void start() {
        this.heartbeat.start();
        this.watcher.start();

        logger.debug("Worker start");
        this.shuffleHandler.start();
    }

    private void blockUntilShutdown() throws InterruptedException {
        this.shuffleHandler.join();
    }

    public static void main(String[] args) throws InterruptedException {
        EtcdService.initClient();

        try {
            InetAddress addr = InetAddress.getLocalHost();
            OpsWorker opsWorker = new OpsWorker(addr.getHostAddress(), addr.getHostName());

            opsWorker.start();

            // For test
            TaskConf task = new TaskConf(true, "test_task_123", "test_job_123",
                    new OpsNode("test_ip", "test_hostname"));
            TaskConf task2 = new TaskConf(true, "test_task_222", "test_job_123",
                    new OpsNode("test_ip", "test_hostname"));
            opsWorker.shuffleHandler.taskComplete(task);
            opsWorker.shuffleHandler.taskComplete(task2);

            opsWorker.blockUntilShutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
