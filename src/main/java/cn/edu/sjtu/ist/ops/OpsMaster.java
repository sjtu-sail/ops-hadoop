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

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;

import com.google.gson.Gson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.sjtu.ist.ops.common.OpsConf;
import cn.edu.sjtu.ist.ops.common.OpsNode;
import cn.edu.sjtu.ist.ops.util.EtcdService;
import cn.edu.sjtu.ist.ops.util.HeartbeatThread;
import cn.edu.sjtu.ist.ops.util.WatcherThread;

public class OpsMaster {

    private static final Logger logger = LoggerFactory.getLogger(OpsMaster.class);
    
    private OpsScheduler scheduler;

    public OpsMaster() {
        OpsNode tmpNode = new OpsNode("localhost", "localhost");
        OpsNode tmpNode1 = new OpsNode("localhost1", "localhost1");
        OpsNode tmpNode2 = new OpsNode("localhost2", "localhost2");
        OpsNode tmpNode3 = new OpsNode("localhost3", "localhost3");
        OpsConf tmpConf = new OpsConf(tmpNode, new ArrayList<>(Arrays.asList(tmpNode1, tmpNode2, tmpNode3)));
        this.scheduler = new OpsScheduler(tmpConf);
      
    }

    public void start() {
        logger.info("Master start");
        this.scheduler.start();
    }

    public void stop() {
        
    }

    private void blockUntilShutdown() throws InterruptedException {
        this.scheduler.join();
    }

    public static void main(String[] args) throws InterruptedException {
        OpsMaster opsMaster = new OpsMaster();
        EtcdService.initClient();

        try {
            InetAddress addr = InetAddress.getLocalHost();
            OpsNode master = new OpsNode(addr.getHostAddress(), addr.getHostName());
            Gson gson = new Gson();
            HeartbeatThread heartbeatThread = new HeartbeatThread("ops/nodes/master/", gson.toJson(master));
            WatcherThread watcherThread = new WatcherThread("ops/nodes/worker");
            heartbeatThread.start();
            watcherThread.start();

            opsMaster.start();
            opsMaster.blockUntilShutdown();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    
}
