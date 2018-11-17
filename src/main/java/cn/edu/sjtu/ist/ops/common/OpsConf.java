package cn.edu.sjtu.ist.ops.common;

import java.util.List;

public class OpsConf {
    private List<OpsNode> workers;
    private OpsNode master;
    private int portMasterGRPC = 14010;
    private int portWorkerGRPC = 14020;

    public OpsConf(OpsNode master, List<OpsNode> workers) {
        this.master = master;
        this.workers = workers;
    }

    public List<OpsNode> getWorkers() {
        return this.workers;
    }

    public void setWorkers(List<OpsNode> workers) {
        this.workers = workers;
    }

    public OpsNode getMaster() {
        return this.master;
    }

    public void setMaster(OpsNode master) {
        this.master = master;
    }

    public int getPortMasterGRPC() {
        return this.portMasterGRPC;
    }

    public int getPortWorkerGRPC() {
        return this.portWorkerGRPC;
    }

}