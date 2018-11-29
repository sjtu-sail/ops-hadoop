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

public class OpsConf {
    private final OpsNode master;
    private final String localDir;
    private final int portMasterGRPC;
    private final int portWorkerGRPC;

    public OpsConf(OpsNode master, String dir, int portMasterGRPC, int portWorkerGRPC) {
        this.master = master;
        this.localDir = dir;
        this.portMasterGRPC = portMasterGRPC;
        this.portWorkerGRPC = portWorkerGRPC;
    }

    public OpsNode getMaster() {
        return this.master;
    }

    public String getDir() {
        return this.localDir;
    }

    public int getPortMasterGRPC() {
        return this.portMasterGRPC;
    }

    public int getPortWorkerGRPC() {
        return this.portWorkerGRPC;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}