package com.example.zk.alidemo;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 授权失败
 *
 * @author nileader/nileader@gmail.com
 */
public class AuthFailedEvent implements Watcher {

    private AtomicInteger seq = new AtomicInteger();

    private static final Logger LOG = LoggerFactory.getLogger(AllZooKeeperWatcher.class);

    private static final int SESSION_TIMEOUT = 10000;
    private static final String CONNECTION_STRING = "localhost:2181";
    private static final String ZK_PATH = "/nileader";
    private static final String CHILDREN_PATH = "/nileader/ch";
    private static final String LOG_PREFIX_OF_MAIN = "【Main】";

    private final static String authentication_type = "digest";

    private final static String correctAuthentication = "taokeeper:true";

    private ZooKeeper zk = null;

    private CountDownLatch connectedSemaphore = new CountDownLatch(1);

    /**
     * 创建ZK连接
     *
     * @param connectString  ZK服务器地址列表
     * @param sessionTimeout Session超时时间
     */
    public void createConnection(String connectString, int sessionTimeout, String authentication) {
        this.releaseConnection();
        try {
            zk = new ZooKeeper(connectString, sessionTimeout, this);
            zk.addAuthInfo(authentication_type, authentication.getBytes());
            LOG.info(LOG_PREFIX_OF_MAIN + "开始连接ZK服务器");
            connectedSemaphore.await();
        } catch (Exception e) {
        }
    }

    /**
     * 关闭ZK连接
     */
    public void releaseConnection() {
        if (this.zk != null) {
            try {
                this.zk.close();
            } catch (InterruptedException e) {
            }
        }
    }

    /**
     * 创建节点
     *
     * @param path 节点path
     * @param data 初始数据内容
     * @return
     */
    public boolean createPath(String path, String data) {
        try {
            LOG.info(LOG_PREFIX_OF_MAIN + "节点创建成功, Path: "
                    + this.zk.create(path, //
                    data.getBytes(), //
                    Ids.CREATOR_ALL_ACL, //
                    CreateMode.PERSISTENT)
                    + ", content: " + data);
        } catch (Exception e) {
        }
        return true;
    }

    /**
     * 读取指定节点数据内容
     *
     * @param path 节点path
     * @return
     */
    public String readData(String path, boolean needWatch) {
        try {
            return new String(this.zk.getData(path, needWatch, null));
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * 删除指定节点
     *
     * @param path 节点path
     */
    public void deleteNode(String path) {
        try {
            this.zk.delete(path, -1);
            LOG.info(LOG_PREFIX_OF_MAIN + "删除节点成功，path：" + path);
        } catch (Exception e) {
            //TODO
        }
    }

    public void deleteAllTestPath() {
        this.deleteNode(CHILDREN_PATH);
        this.deleteNode(ZK_PATH);
    }


    public static void main(String[] args) throws InterruptedException {

        PropertyConfigurator.configure("src/main/resources/log4j.properties");

        AuthFailedEvent sample = new AuthFailedEvent();
        sample.createConnection(CONNECTION_STRING, SESSION_TIMEOUT, correctAuthentication);

        AuthFailedEvent sample2 = new AuthFailedEvent();
        sample2.createConnection(CONNECTION_STRING, SESSION_TIMEOUT, "");

        //清理节点
        sample.deleteAllTestPath();
        if (sample.createPath(ZK_PATH, System.currentTimeMillis() + "")) {
            //读取数据
            sample2.readData(ZK_PATH, true);
        }
        Thread.sleep(300000);
        sample.releaseConnection();
    }

    /**
     * 收到来自Server的Watcher通知后的处理。
     */
    @Override
    public void process(WatchedEvent event) {

        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (event == null) {
            return;
        }
        //连接状态
        KeeperState keeperState = event.getState();
        //事件类型
        EventType eventType = event.getType();
        //受影响的path
        String path = event.getPath();
        String logPrefix = "【Watcher-" + this.seq.incrementAndGet() + "】";

        LOG.info(logPrefix + "收到Watcher通知");
        LOG.info(logPrefix + "连接状态:\t" + keeperState.toString());
        LOG.info(logPrefix + "事件类型:\t" + eventType.toString());

        if (KeeperState.SyncConnected == keeperState) {
            //成功连接上ZK服务器
            if (EventType.None == eventType) {
                LOG.info(logPrefix + "成功连接上ZK服务器");
                connectedSemaphore.countDown();
            } else if (EventType.NodeCreated == eventType) {
                LOG.info(logPrefix + "节点创建");
            } else if (EventType.NodeDataChanged == eventType) {
                LOG.info(logPrefix + "节点数据更新");
                LOG.info(logPrefix + "数据内容: " + this.readData(ZK_PATH, true));
            } else if (EventType.NodeChildrenChanged == eventType) {
                LOG.info(logPrefix + "子节点变更");
            } else if (EventType.NodeDeleted == eventType) {
                LOG.info(logPrefix + "节点 " + path + " 被删除");
            }

        } else if (KeeperState.Disconnected == keeperState) {
            LOG.info(logPrefix + "与ZK服务器断开连接");
        } else if (KeeperState.AuthFailed == keeperState) {
            LOG.info(logPrefix + "权限检查失败");
        } else if (KeeperState.Expired == keeperState) {
            LOG.info(logPrefix + "会话失效");
        }

        LOG.info("--------------------------------------------");

    }

}
