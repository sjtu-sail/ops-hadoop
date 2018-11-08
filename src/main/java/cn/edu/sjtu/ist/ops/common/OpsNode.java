package cn.edu.sjtu.ist.ops.common;


public class OpsNode {
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

}
