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

import java.io.Serializable;

import com.google.gson.Gson;

public class OpsNode implements Serializable {

    private static final long serialVersionUID = -8745866775809014881L;

    private final String ip;

    public OpsNode(String ip) {
        this.ip = ip;
    }

    public String getIp() {
        return this.ip;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof OpsNode)) {
            return false;
        }
        OpsNode node = (OpsNode) obj;
        return this.ip.equals(node.getIp());
    }

}
