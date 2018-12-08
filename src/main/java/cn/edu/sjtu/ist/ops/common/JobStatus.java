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

import java.util.List;

public class JobStatus {
    public static enum Phase {
        STARTING, MAP, SHUFFLE, REDUCE, CLEANUP
    }

    private final JobConf jobConf;
    private final TaskPreAlloc reducePreAlloc;
    private long startTime; // in ms
    private long finishTime;
    private volatile Phase phase = Phase.MAP; // set initial phase to MAP for now.

    public JobStatus(JobConf jobConf, List<OpsNode> nodes) {
        this.reducePreAlloc = new TaskPreAlloc(jobConf.getNumReduce(), nodes);
        this.startTime = System.currentTimeMillis();
        this.jobConf = jobConf;
    }

    public void mapTaskCompleted(MapConf task) {

    }

    public TaskPreAlloc getReducePreAlloc() {
        return this.reducePreAlloc;
    }

    public JobConf getJobConf() {
        return this.jobConf;
    }

    /**
     * Get start time of the task.
     * 
     * @return 0 is start time is not set, else returns start time.
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Set startTime of the task.
     * 
     * @param startTime start time
     */
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    /**
     * Get task finish time.
     * 
     * @return finish time of the task.
     */
    public long getFinishTime() {
        return finishTime;
    }

    /**
     * Sets finishTime for the task status.
     * 
     * @param finishTime finish time of task.
     */
    void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }
}