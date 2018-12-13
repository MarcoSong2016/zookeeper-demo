package com.example.zk.node;

import com.example.zk.Const;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 临时的顺序节点 (可以用来实现分布式锁)
 * 基本特性与临时节点一致，创建节点的过程中，Zookeeper会在其名字后自动追加一个单调增长的数字后缀，作为新的节点名。回话失效后自动清除。
 */
public class EphemeralSequentialNodeTest {
    private static final Logger logger = LoggerFactory.getLogger(EphemeralSequentialNodeTest.class);

    public static void main(String[] args) throws Exception {
        String path = "/esdata";
        List<String> newPaths = new ArrayList<>();// 已创建的持久顺序节点
        ZooKeeper zk = getZooKeeper();
        logger.info("建立连接");
        zk.create(path, path.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        for (int i = 0; i < 5; i++) {
            // 创建节点
            String createPath = zk.create(path + "/node", path.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
            newPaths.add(createPath);
            logger.info("成功创建节点:" + createPath);
        }
        // 断开连接
        zk.close();
        logger.info("断开连接");
        // 重新连接
        zk = getZooKeeper();
        logger.info("重新连接");
        for (int i = 0; i < 5; i++) {
            logger.info("获取节点[" + newPaths.get(i) + "]PZID:" + zk.exists(newPaths.get(i), false));
        }
    }

    public static ZooKeeper getZooKeeper() throws IOException {
        return new ZooKeeper(Const.IP_ADDRESS, 5000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                logger.info("事件监听:" + event.toString());
            }
        });
    }
}

