package cn.edu.sjtu.ist.ops;

import cn.edu.sjtu.ist.ops.util.*;

public class OpsMaster {
    public static void main(String[] args) throws InterruptedException {
        EtcdService.initClient();
        EtcdService.put("test", "test");
        while (true) {
            System.out.println("hello master.");
            Thread.sleep(5000);
        }
    }
}
