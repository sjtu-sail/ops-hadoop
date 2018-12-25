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
import java.util.List;

import com.coreos.jetcd.data.KeyValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.gson.Gson;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import cn.edu.sjtu.ist.ops.common.JobConf;
import cn.edu.sjtu.ist.ops.common.MapConf;
import cn.edu.sjtu.ist.ops.common.OpsConf;
import cn.edu.sjtu.ist.ops.common.OpsNode;
import cn.edu.sjtu.ist.ops.common.ReduceConf;
import cn.edu.sjtu.ist.ops.util.EtcdService;
import cn.edu.sjtu.ist.ops.util.OpsConfig;
import cn.edu.sjtu.ist.ops.util.OpsUtils;

public class OpsClient {

    private OpsNode master;
    private List<OpsNode> workers = new ArrayList<>();
    private OpsConf opsConf;
    private Gson gson = new Gson();

    public OpsClient() {

        EtcdService.initClient();

        List<KeyValue> workersKV = EtcdService.getKVs(OpsUtils.ETCD_NODES_PATH + "/worker");
        if (workersKV.size() == 0) {
            System.err.println("Workers not found from etcd server.");
            return;
        }
        this.workers = new ArrayList<>();
        workersKV.stream().forEach(kv -> workers.add(gson.fromJson(kv.getValue().toStringUtf8(), OpsNode.class)));

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            OpsConfig opsConfig = mapper.readValue(
                    Thread.currentThread().getContextClassLoader().getResourceAsStream("config.yml"), OpsConfig.class);
            this.master = new OpsNode(opsConfig.getMasterHostName());
            this.opsConf = new OpsConf(master, opsConfig.getOpsWorkerLocalDir(), opsConfig.getOpsMasterPortGRPC(),
                    opsConfig.getOpsWorkerPortGRPC(), opsConfig.getOpsWorkerPortHadoopGRPC());
            System.out.println("opsConf: " + opsConf.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("OpsMaster: " + this.master);
        System.out.println("OpsWorkers: " + this.workers);
    }

    public List<OpsNode> getWorkers() {
        return this.workers;
    }

    public void taskComplete(MapConf task) {
        EtcdService.put(OpsUtils.buildKeyMapCompleted(task.getOpsNode().getIp(), task.getJobId(), task.getTaskId()),
                gson.toJson(task));
    }

    public void registerJob(JobConf job) {
        EtcdService.put(OpsUtils.buildKeyJob(job.getJobId()), gson.toJson(job));
    }

    public void registerReduce(ReduceConf reduce) {
        EtcdService.put(OpsUtils.buildKeyReduceTask(reduce.getOpsNode().getIp(), reduce.getJobId(), reduce.getTaskId()),
                gson.toJson(reduce));
    }

    public static void main(String[] args) throws InterruptedException {
        Thread.currentThread().setName("ops-client");
        OpsClient opsClient = new OpsClient();

        // Command line parser
        CommandLine commandLine;
        Option help = new Option("help", "print this message");
        int tcArgNum = 5;
        Option taskComplete = OptionBuilder.withArgName("taskcomplete").hasArgs().hasArgs(tcArgNum)
                .withDescription("Write TaskComplete to ETCD").create("tc");
        int rjArgNum = 3;
        Option registerJob = OptionBuilder.withArgName("registerjob").hasArgs().hasArgs(rjArgNum)
                .withDescription("Write RegisterJob to ETCD").create("rj");
        int rrArgNum = 3;
        Option registerReduce = OptionBuilder.withArgName("registerreduce").hasArgs().hasArgs(rrArgNum)
                .withDescription("Write RegisterJob to ETCD").create("rr");

        Options options = new Options();
        CommandLineParser parser = new BasicParser();

        options.addOption(help);
        options.addOption(registerJob);
        options.addOption(registerReduce);
        options.addOption(taskComplete);

        try {
            InetAddress addr = InetAddress.getLocalHost();
            String[] rjArgs = { "-rj", "jobid_test", "2", "2" };
            String[] tcArgs = { "-tc", "taskid_test", "jobid_test", addr.getHostAddress(),
                    "/Users/admin/Documents/OPS/application_1544151629395_0001/attempt_1544151629395_0001_m_000001_0/file.out",
                    "/Users/admin/Documents/OPS/application_1544151629395_0001/attempt_1544151629395_0001_m_000001_0/file.out.index" };
            String[] rrArgs = { "-rr", "reduce_test4", "jobid_test", addr.getHostAddress() };

            // commandLine = parser.parse(options, rjArgs);
            // commandLine = parser.parse(options, tcArgs);
            // commandLine = parser.parse(options, rrArgs);
            commandLine = parser.parse(options, args);

            if (commandLine.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("OpsClient", options);

            } else if (commandLine.hasOption("tc")) {
                String[] vals = commandLine.getOptionValues("tc");
                if (vals.length != tcArgNum) {
                    System.out.println("Required arguments: [taskId, jobId, ip, path, indexPath]");
                    System.out.println("Wrong arguments: " + Arrays.toString(vals));
                    return;
                }
                MapConf task = new MapConf(vals[0], vals[1], new OpsNode(vals[2]), vals[3], vals[4]);
                System.out.println("Do taskComplete: " + task.toString());
                opsClient.taskComplete(task);
            } else if (commandLine.hasOption("rj")) {
                String[] vals = commandLine.getOptionValues("rj");
                if (vals.length != rjArgNum) {
                    System.out.println("Required arguments: [jobId, numMap, numReduce]");
                    System.out.println("Wrong arguments: " + Arrays.toString(vals));
                    return;
                }
                JobConf job = new JobConf(vals[0], Integer.parseInt(vals[1]), Integer.parseInt(vals[2]),
                        opsClient.getWorkers());
                opsClient.registerJob(job);
            } else if (commandLine.hasOption("rr")) {
                String[] vals = commandLine.getOptionValues("rr");
                if (vals.length != rrArgNum) {
                    System.out.println("Required arguments: [TaskId, jobId, nodeIp]");
                    System.out.println("Wrong arguments: " + Arrays.toString(vals));
                    return;
                }
                ReduceConf reduce = new ReduceConf(vals[0], vals[1], new OpsNode(vals[2]));
                opsClient.registerReduce(reduce);
                System.out.println("Register reduce: " + reduce.toString());
            }

        } catch (ParseException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
