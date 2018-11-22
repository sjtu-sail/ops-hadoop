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

public class OpsTask {
    public static enum Type {
        ONSHUFFLE, DISTRIBUTEJOB
    }

    private Type type;
    private JobConf pendingJob;
    private TaskConf pendingTask;

    public OpsTask(JobConf pendingJob) {
        this.type = Type.DISTRIBUTEJOB;
        this.pendingJob = pendingJob;
    }

    public OpsTask(TaskConf pendingTask) {
        this.type = Type.ONSHUFFLE;
        this.pendingTask = pendingTask;
    }

    public Type getType() {
        return this.type;
    }

    public JobConf getPendingJob() {
        return this.pendingJob;
    }

    public TaskConf getPendingTask() {
        return this.pendingTask;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}