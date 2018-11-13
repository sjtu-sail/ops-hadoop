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
