package com.example.zk.basics;

import com.example.zk.basics.handler.*;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description: TODO
 * @Author: XinyuTian
 * @Date: 2018/11/13
 */
public class ZnodeDemo {
    static final String nodeRoot = "/myJavaRoot";
    static final String pNode = "p-node";
    static final String psNode = "ps-node";
    static final String eNode = "e-node";

    static String getNodePath(String nodeName) {
        return nodeRoot + "/" + nodeName;
    }

    public static void main(String[] args) throws InterruptedException {
        ZKConnector conn = new ZKConnector();
        ZooKeeper zk = null;
        try {
            zk = conn.connect("localhost:2181");
            ZnodeHandler handler = new ZnodeHandler(zk);
            handler.create(nodeRoot, "root");
            handler.create(getNodePath(pNode), "persistent-node", CreateMode.PERSISTENT);
            handler.create(getNodePath(psNode), "sequential-node", CreateMode.PERSISTENT_SEQUENTIAL);
            handler.create(getNodePath(eNode), "ephemeral-node", CreateMode.EPHEMERAL);

            System.out.println("persistent node exists:" + handler.znodeExists(getNodePath(pNode)));

            List<String> snodes = new ArrayList<>();
            for (String c : handler.getChildren(nodeRoot)) {
                if (c.startsWith("ps-node")) {
                    snodes.add(c);
                }
                System.out.println("child of root:" + c);
            }

            String oldVal = handler.getData(getNodePath(pNode), false);
            System.out.println("persistent node OLD value:" + oldVal);
            handler.setData(getNodePath(pNode), oldVal + "_NEW");
            System.out.println("persistent node NEW value:" + handler.getData(getNodePath(pNode), false));

            try {
                for (String sn : snodes) {
                    handler.delete(getNodePath(sn));
                }
                handler.delete(nodeRoot);
            } catch (KeeperException.NotEmptyException e) {
                System.out.println("delete node FAIL!" + e.getMessage());
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        } finally {
            zk.close();
            conn.close();
        }

    }
}
