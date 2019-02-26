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
import java.net.UnknownHostException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.gson.Gson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.sjtu.ist.ops.common.OpsConf;
import cn.edu.sjtu.ist.ops.common.OpsNode;
import cn.edu.sjtu.ist.ops.util.EtcdService;
import cn.edu.sjtu.ist.ops.util.HeartbeatThread;
import cn.edu.sjtu.ist.ops.util.OpsConfig;
import cn.edu.sjtu.ist.ops.util.OpsUtils;
import cn.edu.sjtu.ist.ops.util.WatcherThread;

public class OpsMaster extends OpsNode {

    private static final Logger logger = LoggerFactory.getLogger(OpsMaster.class);

    private OpsScheduler scheduler;
    private HeartbeatThread heartbeat;
    private WatcherThread watcher;

    public OpsMaster(String ip) {
        super(ip);

        Gson gson = new Gson();
        this.heartbeat = new HeartbeatThread(OpsUtils.ETCD_NODES_PATH + "/master/", gson.toJson(this));
        this.watcher = new WatcherThread(OpsUtils.ETCD_NODES_PATH + "/worker");

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            OpsConfig opsConfig = mapper.readValue(
                    Thread.currentThread().getContextClassLoader().getResourceAsStream("config.yml"), OpsConfig.class);
            OpsNode master = new OpsNode(opsConfig.getMasterHostName());
            OpsConf opsConf = new OpsConf(master, opsConfig.getOpsWorkerLocalDir(), opsConfig.getOpsMasterPortGRPC(),
                    opsConfig.getOpsWorkerPortGRPC(), opsConfig.getOpsWorkerPortHadoopGRPC());
            this.scheduler = new OpsScheduler(opsConf, this.watcher);
        } catch (Exception e) {
            e.printStackTrace();
        }
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
            OpsMaster opsMaster = new OpsMaster(addr.getHostName());
            opsMaster.start();
            opsMaster.blockUntilShutdown();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
