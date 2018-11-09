package cn.edu.sjtu.ist.ops;

public class OpsShuffleHandler extends Thread {
    private volatile boolean stopped;

    public void OpsShuffleHandler() {
        stopped = false;

    }

    @Override
    public void run() {
        try {
            while (!stopped && !Thread.currentThread().isInterrupted()) {
                wait();
            

            }
        } catch (InterruptedException e) {
            //TODO: handle exception
        }
    }
}