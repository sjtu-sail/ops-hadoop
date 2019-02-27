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

import java.util.List;

public class CollectionConf {
    private final String host;
    private final String jobId;
    private final String mapId;
    private final List<IndexRecord> records;

    public CollectionConf(String host, String jobId, String mapId, List<IndexRecord> records) {
        this.host = host;
        this.jobId = jobId;
        this.mapId = mapId;
        this.records = records;
    }

    public List<IndexRecord> getRecords() {
        return this.records;
    }

    public String getHost() {
        return host;
    }

    public String getJobId() {
        return jobId;
    }

    public String getMapId() {
        return mapId;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}