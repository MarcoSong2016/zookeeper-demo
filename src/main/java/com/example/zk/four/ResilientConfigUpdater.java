package com.example.zk.four;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.example.zk.ConnectionWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ResilientConfigUpdater extends ConnectionWatcher {
    public static final String PATH = "/config";
    private ChangedActiveKeyValueStore store;
    private Random random = new Random();

    public ResilientConfigUpdater(String hosts) throws IOException, InterruptedException {
        store = new ChangedActiveKeyValueStore();
        store.connect(hosts);
    }

    public void run() throws InterruptedException, KeeperException {
        while (true) {
            String value = random.nextInt(100) + "";
            store.write(PATH, value);
            System.out.printf("Set %s to %s\n", PATH, value);
            TimeUnit.SECONDS.sleep(random.nextInt(10));
        }
    }

    public static void main(String[] args) throws Exception {
        while (true) {
            try {
                ResilientConfigUpdater configUpdater = new ResilientConfigUpdater("127.0.0.1:2181");
                configUpdater.run();
            } catch (KeeperException.SessionExpiredException e) {
                // start a new session
            } catch (KeeperException e) {
                // already retried ,so exit
                e.printStackTrace();
                break;
            }
        }
    }
}
