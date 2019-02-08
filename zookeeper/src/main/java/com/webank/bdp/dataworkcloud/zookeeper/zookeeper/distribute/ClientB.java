package com.webank.bdp.dataworkcloud.zookeeper.zookeeper.distribute;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class ClientB {

    //定义常量
    private static final String CONNECTSTRING = "192.168.40.101:2181";
    private static final String PATH = "/atguigu/distributed";
    private static final int SESSION_TIMEOUT = 50 * 1000;
    //定义实例变量
    private ZooKeeper zk = null;

    //以下为业务方法
    public ZooKeeper startZK() throws IOException {
        return new ZooKeeper(CONNECTSTRING, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
            }
        });
    }

    public String getZNode(String path) throws KeeperException, InterruptedException {
        byte[] byteArray = zk.getData(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    triggerValue(path);
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, new Stat());

        return new String(byteArray);
    }

    public boolean triggerValue(String path) throws KeeperException, InterruptedException {
        byte[] byteArray = zk.getData(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    triggerValue(path);
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, new Stat());
        String newValue = new String(byteArray);

        if ("ClientB".equals(newValue)) {
            System.out.println("BBBBBBBBBB System is run......");
        }

        return false;
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ClientB test = new ClientB();

        test.setZk(test.startZK());

        System.out.println("************: " + test.getZNode(PATH));

        Thread.sleep(Long.MAX_VALUE);

    }

    //setter---getter
    public ZooKeeper getZk() {
        return zk;
    }

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }
}
