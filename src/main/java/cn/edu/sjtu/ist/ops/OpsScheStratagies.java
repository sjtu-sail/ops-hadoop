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

import java.util.List;

import cn.edu.sjtu.ist.ops.common.CollectionConf;
import cn.edu.sjtu.ist.ops.common.JobConf;
import cn.edu.sjtu.ist.ops.common.OpsNode;
import cn.edu.sjtu.ist.ops.common.MapTaskAlloc;
import cn.edu.sjtu.ist.ops.common.ReduceTaskAlloc;

public class OpsScheStratagies {
    // Maps scheduling stratagies
    public static MapTaskAlloc balancedMaps(JobConf job) {
        // TODO: Schedule jobs here.

        // naive strategy
        // mapTaskAlloc
        MapTaskAlloc mapTaskAlloc = new MapTaskAlloc(job);
        int mapPerNode = job.getNumMap() / job.getWorkers().size();
        int mapRemainder = job.getNumMap() % job.getWorkers().size();

        for (int i = 0; i < job.getWorkers().size(); i++) {
            OpsNode worker = job.getWorkers().get(i);
            if(mapRemainder > 0) {
                mapTaskAlloc.addMapPreAlloc(worker.getIp(), mapPerNode + 1);
                mapRemainder--;
            } else {
                mapTaskAlloc.addMapPreAlloc(worker.getIp(), mapPerNode);
            }
        }

        return mapTaskAlloc;
    }

    public static MapTaskAlloc unbalancedMaps(JobConf job) {
        // TODO: Schedule jobs here.

        // naive strategy
        // mapTaskAlloc
        MapTaskAlloc mapTaskAlloc = new MapTaskAlloc(job);
        int mapPerNode = job.getNumMap() / job.getWorkers().size();
        int mapRemainder = job.getNumMap() % job.getWorkers().size();

        String host = job.getWorkers().get(0).getIp();
        mapTaskAlloc.addMapPreAlloc(host, mapPerNode + mapRemainder);
        for (int i = 1; i < job.getWorkers().size(); i++) {
            OpsNode worker = job.getWorkers().get(i);
            mapTaskAlloc.addMapPreAlloc(worker.getIp(), mapPerNode);
        }

        return mapTaskAlloc;

        // logger.debug("Put MapTaskAllocMapping and put etcd. Job: " 
        //         + job.getJobId() + " MapTaskAlloc: " + gson.toJson(mapTaskAlloc));
        // this.mapTaskAllocMapping.put(job.getJobId(), mapTaskAlloc);

        // EtcdService.put(OpsUtils.buildKeyMapTaskAlloc(job.getJobId()), gson.toJson(mapTaskAlloc));

    }

    // Reduces scheduling stratagies
    public static ReduceTaskAlloc balancedReduces(JobConf job, List<CollectionConf> CollectionList) {
        // reduce
        ReduceTaskAlloc reduceTaskAlloc = new ReduceTaskAlloc(job);
        int reducePerNode = job.getNumReduce() / job.getWorkers().size();
        int reduceRemainder = job.getNumReduce() % job.getWorkers().size();

        for (int i = 1; i < job.getWorkers().size(); i++) {
            OpsNode worker = job.getWorkers().get(i);
            if(reduceRemainder > 0) {
                reduceTaskAlloc.addReducePreAlloc(worker.getIp(), reducePerNode + 1);
                reduceRemainder--;
            } else {
                reduceTaskAlloc.addReducePreAlloc(worker.getIp(), reducePerNode);
            }
        }

        return reduceTaskAlloc;
    } 

    public static ReduceTaskAlloc unbalancedReduces(JobConf job, List<CollectionConf> CollectionList) {
        // reduce
        ReduceTaskAlloc reduceTaskAlloc = new ReduceTaskAlloc(job);
        int reducePerNode = job.getNumReduce() / job.getWorkers().size();
        int reduceRemainder = job.getNumReduce() % job.getWorkers().size();
        String host = job.getWorkers().get(0).getIp();
        host = job.getWorkers().get(0).getIp();
        reduceTaskAlloc.addReducePreAlloc(host, reducePerNode + reduceRemainder);
        for (int i = 0; i < job.getWorkers().size(); i++) {
            OpsNode worker = job.getWorkers().get(i);
            reduceTaskAlloc.addReducePreAlloc(worker.getIp(), reducePerNode);
        }

        return reduceTaskAlloc;

        // logger.debug("Put ReduceTaskAllocMapping & put etcd Job: " 
        //         + job.getJobId() + " ReduceTaskAlloc: " + gson.toJson(reduceTaskAlloc));
        // this.reduceTaskAllocMapping.put(job.getJobId(), reduceTaskAlloc);

        // EtcdService.put(OpsUtils.buildKeyReduceTaskAlloc(job.getJobId()), gson.toJson(reduceTaskAlloc));
    } 
}