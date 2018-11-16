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

package cn.edu.sjtu.ist.ops.util;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class OpsConf {
    @JsonProperty
    private Ops ops;

    @JsonProperty
    private Etcd etcd;

    public Ops getOps() {
        return ops;
    }

    public Etcd getEtcd() {
        return etcd;
    }
}

class Ops {
    @JsonProperty
    private Master master;

    @JsonProperty
    private Grpc grpc;

    public Master getMaster() {
        return master;
    }

    public Grpc getGrpc() {
        return grpc;
    }
}

class Master {
    @JsonProperty
    private String hostname;

    @JsonProperty
    private Integer port;

    public String getHostname() {
        return hostname;
    }

    public Integer getPort() {
        return port;
    }
}

class Grpc {
    @JsonProperty
    private String master;

    @JsonProperty
    private String worker;

    public String getMaster() {
        return master;
    }

    public String getWorker() {
        return worker;
    }
}

class Etcd {
    @JsonProperty
    private List<Endpoint> endpoints;

    public List<Endpoint> getEndpoints() {
        return endpoints;
    }
}

class Endpoint {
    @JsonProperty
    private String hostname;

    @JsonProperty
    private Integer port;

    public String getHostname() {
        return hostname;
    }

    public Integer getPort() {
        return port;
    }
}
