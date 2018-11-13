package com.example.zk.basics.handler;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;

/**
 * @Description: TODO
 * @Author: XinyuTian
 * @Date: 2018/11/13
 */
public class ZnodeHandler {
    private static final CreateMode DEFAULT_MODE = CreateMode.PERSISTENT;
    private static final List<ACL> DEFAULT_ACL = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    private static final Charset utf8 = Charset.forName("UTF-8");

    private ZooKeeper zk;

    public ZnodeHandler(ZooKeeper zk) {
        this.zk = zk;
    }

    public void create(String path, String data) throws KeeperException, InterruptedException {
        try {
            zk.create(path, data.getBytes(utf8), DEFAULT_ACL, DEFAULT_MODE);
        } catch (KeeperException.NodeExistsException e) {
            // ignore
        }
    }

    public void create(String path, String data, CreateMode mode) throws KeeperException, InterruptedException {
        try {
            zk.create(path, data.getBytes(utf8), DEFAULT_ACL, mode);
        } catch (KeeperException.NodeExistsException e) {
            // ignore
        }
    }

    public void create(String path, String data, CreateMode mode, List<ACL> acls) throws KeeperException,
            InterruptedException {
        try {
            zk.create(path, data.getBytes(utf8), acls, mode);
        } catch (KeeperException.NodeExistsException e) {
            // ignore
        }
    }

    public void delete(String nodePath) throws KeeperException, InterruptedException {
        try {
            zk.delete(nodePath,zk.exists(nodePath,true).getVersion());
        } catch (NullPointerException e) {
            // ignore
        }
    }

    public List<String> getChildren(String nodePath) throws KeeperException, InterruptedException {
        if (zk.exists(nodePath,true) != null) {
            return zk.getChildren(nodePath, false);
        }
        return Collections.emptyList();
    }

    public boolean znodeExists(String path) throws KeeperException,InterruptedException {
        Stat stat = zk.exists(path, true);
        return stat != null;
    }

    public String getData(String nodePath, boolean withWatch) throws KeeperException, InterruptedException,
            UnsupportedEncodingException {
        if (zk.exists(nodePath, withWatch) != null) {
            if (withWatch) {

            } else {
                byte[] data = zk.getData(nodePath, false, null);
                if (data != null) {
                    return new String(data, utf8);
                }
            }
        }
        return null;
    }

    public void setData(String nodePath, String data) throws KeeperException, InterruptedException {
        zk.setData(nodePath, data.getBytes(utf8), zk.exists(nodePath,true).getVersion());
    }
}
