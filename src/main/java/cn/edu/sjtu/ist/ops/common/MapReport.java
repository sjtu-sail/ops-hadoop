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

public class MapReport extends MapConf {
    private final Long outputDataSize;
    private final Long mapFinishTime;

    public MapReport(String taskId, String jobId, OpsNode node, String path, String indexPath, Long outputDataSize, Long mapFinishTime) {
        super(taskId, jobId, node, path, indexPath);
        this.outputDataSize = outputDataSize;
        this.mapFinishTime = mapFinishTime;
    }

    public MapReport(MapConf mapconf, Long outputDataSize, Long mapFinishTime) {
        super(mapconf.getTaskId(), mapconf.getJobId(), mapconf.getOpsNode(), mapconf.getPath(), mapconf.getIndexPath());
        this.outputDataSize = outputDataSize;
        this.mapFinishTime = mapFinishTime;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }

    public Long getOutputDataSize() {
        return this.outputDataSize;
    }

    public Long getMapFinishTime() {
        return this.mapFinishTime;
    }
}