package com.webank.bdp.dataworkcloud.zookeeper.zookeeper.distribute;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * 创建/atguigu/distributed 节点
 *启动2个client端
 * set /atguigu/distributed ClientB 或者ClientA 给节点赋值（server端） 可看到变化
 */
public class ClientA {

    //定义常量
    private static final String CONNECTSTRING = "192.168.40.101:2181";
    private static final String PATH = "/atguigu/distributed";
    private static final int SESSION_TIMEOUT = 5000000 * 1000;
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
        //需要递归来不断创建新的watche，如果监听的是其父节点，没有变化，则不需要new新的watcher
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

        if ("ClientA".equals(newValue)) {
            System.out.println("AAA System is run......");
        }

        return false;
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ClientA test = new ClientA();

        test.setZk(test.startZK());

        System.out.println("************: " + test.getZNode(PATH));  //这里只是获取节点数据，没有改值，所以不会触发process的方法

        Thread.sleep(Long.MAX_VALUE);//当节点的值改变后，会触发process的方法，同时放一个新的watcher去对新值进行观察，拿到新值后判断下是不是ClientA，是的话就打印出来

    }

    //setter---getter
    public ZooKeeper getZk() {
        return zk;
    }

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }
}
