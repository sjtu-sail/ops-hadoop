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

package cn.edu.sjtu.ist.ops.common;

import com.google.gson.Gson;
import java.nio.file.Path;

public class TaskConf {
    private final Boolean isMap;
    private final String taskId;
    private final String jobId;
    private final OpsNode opsNode;
    private final Path path;
    private final Path indexPath;

    public TaskConf(Boolean isMap, String taskId, String jobId, OpsNode node, Path path, Path indexPath) {
        this.isMap = isMap;
        this.taskId = taskId;
        this.jobId = jobId;
        this.opsNode = node;
        this.path = path;
        this.indexPath = indexPath;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }

    public Boolean getIsMap() {
        return this.isMap;
    }

    public String getTaskId() {
        return this.taskId;
    }

    public String getJobId() {
        return this.jobId;
    }

    public OpsNode getOpsNode() {
        return this.opsNode;
    }

    public Path getPath() {
        return this.path;
    }

    public Path getIndexPath() {
        return this.indexPath;
    }
}