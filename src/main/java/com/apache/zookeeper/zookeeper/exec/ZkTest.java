package com.apache.zookeeper.zookeeper.exec;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZkTest {

    private ZooKeeper zkClient = null;
    private final static Log logger = LogFactory.getLog(ZkTest.class);


    @Before
    public void init () throws IOException {
        // connectString 连接
        // sessionTimeout 客户端访问 zk 超时参数
        // Watcher 注册监听器，所有 create、delete、getChildren、getStat set 在允许调用触发器时，都会触发 process 逻辑
        zkClient = new ZooKeeper ( "hadoop01:2181,hadoop02:2181,hadoop03:2181" , 2000 , new Watcher ( ) {
            @Override
            public void process ( WatchedEvent watchedEvent ) {
                logger.info (String.format ( "path: %s, type: %s, state: %s, string: %s",watchedEvent.getPath (),watchedEvent.getType (),watchedEvent.getState (),watchedEvent.toString () ));
            }
        } );
    }

    @After
    public void close() throws InterruptedException {
        zkClient.close ();
        logger.info ( "zkClient close ." );
    }

    @Test
    public void createNode () throws KeeperException, InterruptedException {
        String info = null;
        // 1. 创建节点必须存储数据，不能创建没有数据存储的节点
        // 2. 临时节点EPHEMERAL，客户端会话关闭，节点消失，且临时节点不能创建子节点
        // 3. 序列化节点SEQUENTIAL，在给定名称基础上，自动添加自增序列，避免名称重复
        // 4. 持久化节点PERSISTENT，客户端会话关闭，节点依旧保留
        // 创建开放权限的节点
        // 创建临时节点（session 会话关闭，节点消失）
        // info = zkClient.create ( "/servers" , "a".getBytes ( ) , ZooDefs.Ids.OPEN_ACL_UNSAFE , CreateMode.EPHEMERAL );
        // logger.info ( info );

        // 创建序列化 临时节点 （自增序列）
        info = zkClient.create ( "/servers" , "a".getBytes ( ) , ZooDefs.Ids.OPEN_ACL_UNSAFE , CreateMode.EPHEMERAL_SEQUENTIAL );
        logger.info ( info ); //  /servers0000000010

        // 创建持久化节点 (永久保存)
        info = zkClient.create ( "/servers" , "a".getBytes ( ) , ZooDefs.Ids.OPEN_ACL_UNSAFE , CreateMode.PERSISTENT );
        logger.info ( info ); //  /servers0000000010
    }

    @Test
    public void getChildren() throws KeeperException, InterruptedException {
        // 获取子节点名称，并且注册监听器，当 /servers 子节点发生变化时，触发监听器 （即，命令行执行 create -s /servers/01 'a' 时 会触发一次 Watcher 中的 process，且仅触发一次）
        List<String> children = null;
         children = zkClient.getChildren ( "/servers" , true );
         logger.info ( children );

        // 获取子节点，但未注册监听器，子节点变化，不会执行 Watcher process 逻辑
//        children = zkClient.getChildren ( "/servers" , false );
//        logger.info ( children );

        Stat stat = new Stat ( ); // 提取造好容器，准备存储信息
        zkClient.getChildren ( "/servers",true,stat ); // 获取子节点，注册监听器同时获取节点信息

        Thread.sleep ( Long.MAX_VALUE );

    }

    @Test
    public void getValue() throws KeeperException, InterruptedException {
        Stat stat = new Stat ( );
        byte[] data = zkClient.getData ( "/servers" , true , stat );
        logger.info (String.format ( "data: %s, ctimne: %s, pzxid: %s",new String(data),stat.getCtime (),stat.getPzxid () ));
    }

    @Test
    public void setValue() throws KeeperException, InterruptedException {
        // 对指定版本数据进行更新操作, -1 是告诉服务端基于最新版本进行更新操作
        Stat stat = new Stat ( );
        byte[] data = zkClient.getData ( "/servers" , false , stat );
        logger.info ( String.format (  "version: %s, data: %s ", stat.getVersion (),new String ( data ))); // 获取老版本

        stat = zkClient.setData ( "/servers" , "aa".getBytes ( ) , -1 ); // 在最新基础上更新
        logger.info ( String.format (  "version: %s ", stat.getVersion ()));

        data = zkClient.getData ( "/servers" , false , stat );
        logger.info ( String.format (  "version: %s, data: %s ", stat.getVersion (),new String ( data ))); // 获取最新版本
    }

    @Test
    public void exists() throws KeeperException, InterruptedException {
        // 存在直接返回节点状态，否则返回 null
        Stat stat = zkClient.exists ( "/servers1" , true );
        logger.info ( null == stat? null: stat.toString () );
    }

    @Test
    public void getAcl() throws KeeperException, InterruptedException {
        // 存在直接返回节点状态，否则返回 null
        Stat stat = new Stat ( );
        List<ACL> acls = zkClient.getACL ( "/servers" , stat );
        for(ACL acl: acls ){
            logger.info ( String.format ("id: %s, perms: %s" , acl.getId (),acl.getPerms ()));
        }
    }

    @Test
    public void tx() throws KeeperException, InterruptedException {
        List<OpResult> results = null;
        // 事物
        // 方案 1：手动定义
//        List<Op> ops  = new ArrayList<> (  );
//        boolean add = ops.add ( Op.create ( "/servers" , "01".getBytes ( ) ,ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL ));
//        boolean set = ops.add ( Op.setData ( "/servers" , "02".getBytes ( ) , -1));
//        results = zkClient.multi ( ops );
//
//        for(OpResult res:results){
//            logger.info (  res.getType ());
//        }

        // 方案 2：自己封装
        Transaction transaction = zkClient.transaction ( );

        results = transaction.create ( "/servers01","hello".getBytes (), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
                .setData ( "/servers01","hi".getBytes (),-1 )
                .check ( "/servers01",1 )
                .delete ( "/servers01",1 )
                .commit ();

        for(OpResult res:results){
            logger.info ( res);
        }


    }






}
