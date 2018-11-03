package cn.edu.sjtu.ist.ops.util;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.lease.LeaseGrantResponse;
import com.coreos.jetcd.lease.LeaseKeepAliveResponse;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.PutOption;

public class EtcdService {
    private static Client client = null;
    private static long leaseId = 0L;

    public static Client getClient() {
        return client;
    }

    /**
     * 
     */
    public static synchronized void initClient() {
        if (null == client) {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            Properties props = new Properties();
            try (InputStream resourceStream = loader.getResourceAsStream("ops.properties")) {
                props.load(resourceStream);
            } catch (Exception e) {
                e.printStackTrace();
            }

            client = Client.builder()
                    .endpoints("http://" + props.get("etcd.host").toString() + ":" + props.get("etcd.port").toString())
                    .build();
        }
    }

    /**
     * 
     * @param key
     * @return
     */
    public static String get(String key) {
        try {
            return client.getKVClient().get(ByteSequence.fromString(key)).get().getKvs().get(0).getValue()
                    .toStringUtf8();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static List<String> getAll(String key) {
        GetOption getOption = GetOption.newBuilder().withPrefix(ByteSequence.fromString(key)).build();
        try {
            return client.getKVClient().get(ByteSequence.fromString(key), getOption).get().getKvs().stream()
                    .map((KeyValue keyValue) -> {
                        return keyValue.getKey().toStringUtf8();
                    }).collect(Collectors.toList());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
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
            long leaseId = leaseGrantResponse.get().getID();
            putOption = PutOption.newBuilder().withLeaseId(leaseId).build();
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
    public static void keepAliveOnce(long leaseId) {
        CompletableFuture<LeaseKeepAliveResponse> response = client.getLeaseClient().keepAliveOnce(leaseId);
        System.out.println(response);
    }

    public static Watcher watch(String key) {
        return client.getWatchClient().watch(ByteSequence.fromString(key));
    }

    /**
     * 
     * @param key
     * @param value
     */
    public static void register(String key, String value) {
        if (leaseId == 0) {
            leaseId = lease(key, value, 10L);
        } else {
            keepAliveOnce(leaseId);
        }
    }

}
