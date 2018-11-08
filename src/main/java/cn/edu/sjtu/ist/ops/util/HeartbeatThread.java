package cn.edu.sjtu.ist.ops.util;

public class HeartbeatThread extends Thread {
    private String prefix;
    private String value;

    public HeartbeatThread(String prefix, String value) {
        this.prefix = prefix;
        this.value = value;
    }

    public void run() {
        while (true) {
            EtcdService.register(this.prefix, this.value);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
