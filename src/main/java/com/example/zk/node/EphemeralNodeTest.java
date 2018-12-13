package com.example.zk.node;

import com.example.zk.Const;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 临时节点 (客户端会话失效或连接关闭后，该节点会被自动删除，且不能再临时节点下面创建子节点)
 */
public class EphemeralNodeTest {
    private static final Logger logger = LoggerFactory.getLogger(EphemeralNodeTest.class);

    public static void main(String[] args) throws Exception {
        String path = "/testnode2";
        ZooKeeper zk = getZooKeeper();
        logger.info("建立连接");
        // 创建节点
        String createPath = zk.create(path, path.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        logger.info("成功创建节点:" + createPath);
        // 断开连接
        zk.close();
        logger.info("断开连接");
        // 重新连接
        zk = getZooKeeper();
        logger.info("重新连接");
        logger.info("获取节点[" + path + "]PZID:" + zk.exists(path, false));
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
