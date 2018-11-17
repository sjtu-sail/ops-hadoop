package cn.edu.sjtu.ist.ops.util;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OpsConfig {
    @JsonProperty
    private Etcd etcd;

    public Etcd getEtcd() {
        return etcd;
    }

    public void setEtcd(Etcd etcd) {
        this.etcd = etcd;
    }
}

class Etcd {
    @JsonProperty
    private List<Endpoint> endpoints;

    public List<Endpoint> getEndpoints() {
        return endpoints;
    }

    public void setEndpoints(List<Endpoint> endpoints) {
        this.endpoints = endpoints;
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

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }
}
