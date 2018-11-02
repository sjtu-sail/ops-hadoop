package cn.edu.sjtu.ist.ops.util;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.lease.*;
import com.coreos.jetcd.options.*;

import java.util.concurrent.CompletableFuture;

public class EtcdService {
    private static Client client = null;

    public static Client getClient() {
        return client;
    }
 
    /**
     * 
     */
    public static synchronized void initClient() {
        if (null == client) {

            client = Client.builder().endpoints("http://202.120.40.4:12379").build();
        }
    }

    /**
     * 
     * @param key
     * @return
     */
    public static KeyValue get(String key) {
        KeyValue keyValue = null;
        try {
            keyValue = client.getKVClient().get(ByteSequence.fromString(key)).get().getKvs().get(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return keyValue;
    }
 
    /**
     * 
     * @param key
     * @param value
     */
    public static void put(String key, String value) {
        client.getKVClient().put(ByteSequence.fromString(key), ByteSequence.fromString(value));
    }

    /**
     * 
     * @param key
     * @param value
     * @param ttl
     * @return
     */
    public static long lease(String key, String value, long ttl) {
        CompletableFuture<LeaseGrantResponse> leaseGrantResponse = client.getLeaseClient().grant(ttl);
        PutOption putOption;
        try {
            putOption = PutOption.newBuilder().withLeaseId(leaseGrantResponse.get().getID()).build();
            client.getKVClient().put(ByteSequence.fromString(key), ByteSequence.fromString(value), putOption);
            return leaseGrantResponse.get().getID();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }

    /**
     * 
     * @param leaseId
     */
    public static void keepAlive(long leaseId) {
        getClient().getLeaseClient().keepAlive(leaseId);
    }

}