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

import java.nio.file.Path;
import java.nio.file.Paths;
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
import cn.edu.sjtu.ist.ops.common.OpsConf;
import cn.edu.sjtu.ist.ops.common.OpsNode;
import cn.edu.sjtu.ist.ops.common.TaskConf;
import cn.edu.sjtu.ist.ops.util.EtcdService;
import cn.edu.sjtu.ist.ops.util.OpsConfig;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

public class OpsClient {

    private OpsNode master = new OpsNode("localhost", "localhost");
    private List<OpsNode> workers = new ArrayList<>();
    private OpsConf opsConf;
    private ManagedChannel masterChannel;
    private ManagedChannel workerChannel;
    private ManagedChannel hadoopChannel;
    private OpsInternalGrpc.OpsInternalStub workerStub;
    private OpsInternalGrpc.OpsInternalStub masterStub;
    private HadoopOpsGrpc.HadoopOpsStub hadoopStub;

    public OpsClient() {

        EtcdService.initClient();
        Gson gson = new Gson();

        List<KeyValue> masterKV = EtcdService.getKVs("ops/nodes/master");
        if (masterKV.size() == 0) {
            System.err.println("Master not found from etcd server.");
            return;
        }
        this.master = gson.fromJson(masterKV.get(0).getValue().toStringUtf8(), OpsNode.class);

        List<KeyValue> workersKV = EtcdService.getKVs("ops/nodes/worker");
        if (workersKV.size() == 0) {
            System.err.println("Workers not found from etcd server.");
            return;
        }
        this.workers = new ArrayList<>();
        workersKV.stream().forEach(kv -> workers.add(gson.fromJson(kv.getValue().toStringUtf8(), OpsNode.class)));

        System.out.println("OpsMaster: " + this.master);
        System.out.println("OpsWorkers: " + this.workers);

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            OpsConfig opsConfig = mapper.readValue(
                    Thread.currentThread().getContextClassLoader().getResourceAsStream("config.yml"), OpsConfig.class);
            this.master = new OpsNode(opsConfig.getMasterHostName(), opsConfig.getMasterHostName());
            this.opsConf = new OpsConf(master, opsConfig.getOpsWorkerLocalDir(), opsConfig.getOpsMasterPortGRPC(),
                    opsConfig.getOpsWorkerPortGRPC(), opsConfig.getOpsWorkerPortHadoopGRPC());
            System.out.println("opsConf: " + opsConf.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public List<OpsNode> getWorkers() {
        return this.workers;
    }

    public void taskComplete(TaskConf task) {
        this.hadoopChannel = ManagedChannelBuilder.forAddress(task.getOpsNode().getIp(), opsConf.getPortHadoopGRPC())
                .usePlaintext().build();
        this.hadoopStub = HadoopOpsGrpc.newStub(this.hadoopChannel);
        HadoopMessage request = HadoopMessage.newBuilder().setIsMap(task.getIsMap()).setTaskId(task.getTaskId())
                .setJobId(task.getJobId()).setPath(task.getPath().toString())
                .setIndexPath(task.getIndexPath().toString()).build();
        StreamObserver<Empty> requestObserver = new StreamObserver<Empty>() {
            @Override
            public void onNext(Empty msg) {

            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {
                hadoopChannel.shutdown();
            }
        };
        hadoopStub.notify(request, requestObserver);
    }

    public void registerJob(JobConf job) {
        this.masterChannel = ManagedChannelBuilder
                .forAddress(this.opsConf.getMaster().getIp(), opsConf.getPortMasterGRPC()).usePlaintext().build();
        this.masterStub = OpsInternalGrpc.newStub(this.masterChannel);
        StreamObserver<JobMessage> requestObserver = masterStub.registerJob(new StreamObserver<JobMessage>() {
            @Override
            public void onNext(JobMessage msg) {
                System.out.println("ShuffleHandler: " + msg.getJobConf());
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {
                masterChannel.shutdown();
            }
        });

        try {
            Gson gson = new Gson();
            System.out.println(gson.toJson(job));
            JobMessage message = JobMessage.newBuilder().setJobConf(gson.toJson(job)).build();
            requestObserver.onNext(message);
        } catch (RuntimeException e) {
            // Cancel RPC
            requestObserver.onError(e);
            throw e;
        }
        // Mark the end of requests
        requestObserver.onCompleted();
    }

    public static void main(String[] args) throws InterruptedException {
        Thread.currentThread().setName("ops-client");
        OpsClient opsClient = new OpsClient();

        // Command line parser
        CommandLine commandLine;
        Option help = new Option("help", "print this message");
        int tcArgNum = 7;
        Option taskComplete = OptionBuilder.withArgName("taskcomplete").hasArgs().hasArgs(tcArgNum)
                .withDescription("Send a TaskComplete message to master").create("tc");
        int rjArgNum = 3;
        Option registerJob = OptionBuilder.withArgName("registerjob").hasArgs().hasArgs(rjArgNum)
                .withDescription("Send a RegisterJob message to master").create("rj");

        Options options = new Options();
        CommandLineParser parser = new BasicParser();

        String[] rjArgs = { "-rj", "jobid-test", "2", "2" };
        String[] tcArgs = { "-tc", "1", "taskid-test", "jobid-test", "10.211.55.2", "Administrators-MacBook-Pro.local",
                "/testpath", "/testindexpath" };

        options.addOption(help);
        options.addOption(registerJob);
        options.addOption(taskComplete);

        try {
            // commandLine = parser.parse(options, rjArgs);
            commandLine = parser.parse(options, tcArgs);
            // commandLine = parser.parse(options, args);

            if (commandLine.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("OpsClient", options);

            } else if (commandLine.hasOption("tc")) {
                String[] vals = commandLine.getOptionValues("tc");
                if (vals.length != tcArgNum) {
                    System.out.println("Required arguments: [isMap, taskId, jobId, ip, hostname, path, indexPath]");
                    System.out.println("Wrong arguments: " + Arrays.toString(vals));
                    return;
                }
                Path path = Paths.get(vals[5]);
                Path indexPath = Paths.get(vals[6]);
                TaskConf task = new TaskConf(true, vals[1], vals[2], new OpsNode(vals[3], vals[4]), path, indexPath);
                System.out.println("Do taskComplete: ");
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
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

}
