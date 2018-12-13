package com.example.zk.node;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 持久节点 (节点创建后，会一直存在，不会因客户端会话失效而删除)
 * https://laoyuan.me/posts/zookeeper-node-type.html
 */
public class PersistentNodeTest {
    private static final Logger logger = LoggerFactory.getLogger(PersistentNodeTest.class);

    public static void main(String[] args) throws Exception {
        String path = "/testnode1";
        ZooKeeper zk = getZooKeeper();
        logger.info("建立连接");
        // 创建节点
        String createPath = zk.create(path, path.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        logger.info("成功创建节点:" + createPath);
        // 断开连接
        zk.close();
        logger.info("断开连接");
        // 重新连接
        zk = getZooKeeper();
        logger.info("重新连接");
        logger.info("获取节点[" + path + "]的PZID:" + zk.exists(path, false).getPzxid());
    }

    public static ZooKeeper getZooKeeper() throws IOException {
        return new ZooKeeper("127.0.0.1:2181", 5000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                logger.info("事件监听:" + event.toString());
            }
        });
    }
}
