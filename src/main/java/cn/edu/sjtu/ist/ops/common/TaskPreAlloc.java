/*
 * Copyright 2018 SJTU IST Lab
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package cn.edu.sjtu.ist.ops.common;

import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TaskPreAlloc {
    private final Integer numTasks;
    private final Integer numNodes;
    private Map<String, OpsNode> nodesMap = new HashMap<>();
    private Map<String, List<Integer>> taskOrder = new HashMap<>();

    public TaskPreAlloc(Integer numTasks, List<OpsNode> nodes) {
        this.numTasks = numTasks;
        this.numNodes = nodes.size();
        if (this.numNodes == 0) {
            return;
        }
        for (OpsNode node : nodes) {
            this.nodesMap.put(node.getIp(), node);
            this.taskOrder.put(node.getIp(), new ArrayList<>());
        }
        Iterator<List<Integer>> iter = this.taskOrder.values().iterator();
        for (Integer i = 0; i < numTasks; i++) {
            iter.next().add(i);
            if (!iter.hasNext()) {
                iter = this.taskOrder.values().iterator();
            }
        }
    }

    public Map<String, OpsNode> getNodesMap() {
        return this.nodesMap;
    }

    public List<Integer> getTaskOrder(String ip) {
        return taskOrder.get(nodesMap.get(ip).getIp());
    }

    public Integer getNumTasks() {
        return this.numTasks;
    }

    public Integer getNumNodes() {
        return this.numNodes;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this.taskOrder);
    }

    // public static void main(String[] args) throws InterruptedException {
    // List<OpsNode> nodes = new ArrayList<>();
    // System.out.println("hello");
    // for (int i = 0; i < 4; i++) {
    // System.out.println(nodes.size());
    // nodes.add(new OpsNode("ip-" + i, "hostname-" + i));
    // }
    // TaskPreAlloc preAlloc = new TaskPreAlloc(11, nodes);
    // System.out.println(preAlloc.toString());
    // }
}