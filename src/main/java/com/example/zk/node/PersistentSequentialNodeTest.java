package com.example.zk.node;

import com.example.zk.Const;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 持久的顺序节点
 * <p>
 * 持久的顺序节点基于持久节点类型，额外的特性是：
 * 在ZooKeeper中，每个父节点会为他的第一级子节点维护一份时序，会记录每个子节点创建的先后顺序；
 * ZooKeeper会自动为给定节点名加上一个数字后缀，作为新的节点名，这个数字后缀的范围是整型的最大值。
 */
public class PersistentSequentialNodeTest {
    private static final Logger logger = LoggerFactory.getLogger(PersistentSequentialNodeTest.class);

    public static void main(String[] args) throws Exception {
        String path = "/psdata";
        List<String> newPaths = new ArrayList<>();// 已创建的持久顺序节点
        ZooKeeper zk = getZooKeeper();
        logger.info("建立连接");
        if (zk.exists(path, false) == null) {
            zk.create(path, path.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        for (int i = 0; i < 5; i++) {
            // 创建节点
            String createPath = zk.create(path + "/node", path.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT_SEQUENTIAL);
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
