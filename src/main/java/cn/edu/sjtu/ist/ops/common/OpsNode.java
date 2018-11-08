package cn.edu.sjtu.ist.ops.common;

import java.io.Serializable;

import com.google.gson.Gson;

public class OpsNode implements Serializable {

    private static final long serialVersionUID = -8745866775809014881L;

    private final String ip;
    private final String hostname;

    public OpsNode(String ip, String hostname) {
        this.ip = ip;
        this.hostname = hostname;
    }

    public String getIp() {
        return this.ip;
    }

    public String getHostname() {
        return this.hostname;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
