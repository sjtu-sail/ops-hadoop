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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;

import com.google.gson.Gson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.sjtu.ist.ops.common.OpsConf;
import cn.edu.sjtu.ist.ops.common.OpsNode;
import cn.edu.sjtu.ist.ops.common.TaskConf;
import cn.edu.sjtu.ist.ops.util.EtcdService;
import cn.edu.sjtu.ist.ops.util.HeartbeatThread;

public class OpsWorker {

    private static final Logger logger = LoggerFactory.getLogger(OpsWorker.class);
    private OpsShuffleHandler shuffleHandler;

    public OpsWorker() {
        OpsNode tmpNode = new OpsNode("localhost", "localhost");
        OpsNode tmpNode1 = new OpsNode("localhost1", "localhost1");
        OpsNode tmpNode2 = new OpsNode("localhost2", "localhost2");
        OpsNode tmpNode3 = new OpsNode("localhost3", "localhost3");
        OpsConf tmpConf = new OpsConf(tmpNode, new ArrayList<>(Arrays.asList(tmpNode1, tmpNode2, tmpNode3)));

        shuffleHandler = new OpsShuffleHandler(tmpConf);
    }

    public void start() {
        logger.info("Worker start");
        this.shuffleHandler.start();
    }

    private void blockUntilShutdown() throws InterruptedException {
        this.shuffleHandler.join();
    }

    public static void main(String[] args) throws InterruptedException {
        OpsWorker opsWorker = new OpsWorker();
        EtcdService.initClient();

        try {
            InetAddress addr = InetAddress.getLocalHost();
            OpsNode worker = new OpsNode(addr.getHostAddress(), addr.getHostName());
            Gson gson = new Gson();
            HeartbeatThread thread = new HeartbeatThread("ops/nodes/worker/", gson.toJson(worker));
            thread.start();

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
