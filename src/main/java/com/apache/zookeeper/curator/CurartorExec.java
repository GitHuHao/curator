package com.apache.zookeeper.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.spi.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.omg.CORBA.TIMEOUT;

import java.util.List;

public class CurartorExec {
    /**
     * 测试 Curator 相关特性
     * 注中间件需要注意版本兼容
     * Curator： 4.0.0 与 zookeeper3.5.x 很好兼容，与 zookeeper3.4.x 存储兼容问题
     */

    // 会话超时 30s
    private final int SESSION_TIMEOUT = 30 * 1000;
    // 连接超时 3s
    private final int CONNECTION_TIMROUT = 3 * 1000;

    private final String CONNECT_STRING = "hadoop01:2181,hadoop02:2181,hadoop03:2181";

    private CuratorFramework client = null;

    private boolean withNameSpace = true;

    /*
        连接重试策略 重试 3 次，每次休眠 1s
        ExponentialBackoffRetry：重试一定次数，每次重试时间依次递增
        RetryNTimes：重试N次
        RetryOneTime：重试一次
        RetryUntilElapsed：重试一定时间
     */
    private RetryPolicy retryPolicy = new ExponentialBackoffRetry ( 1000,3 );

    @Before
    public void init(){
        if(withNameSpace){
            client = CuratorFrameworkFactory.builder()
                    .namespace("mydemo/v1")
                    .connectString(CONNECT_STRING)
                    .sessionTimeoutMs(SESSION_TIMEOUT)
                    .connectionTimeoutMs( CONNECTION_TIMROUT )
                    .retryPolicy(retryPolicy)
                    .build();
        }else{
            client = CuratorFrameworkFactory.newClient ( CONNECT_STRING,SESSION_TIMEOUT, CONNECTION_TIMROUT, retryPolicy);

        }
        client.start ();
    }

    @After
    public void close(){
        client.close ();
    }

    @Test
    public void testCreate() throws Exception {
        String result = null;
        // 创建持久化节点
        result = client.create ( ).forPath ( "/curator" , "curator data".getBytes ( ) );
        System.out.println ( result );
        // 创建持久序列化节点
        result = client.create ().withMode ( CreateMode.PERSISTENT_SEQUENTIAL ).forPath ( "/curator_sequential","curator_sequential".getBytes () );
        System.out.println ( result );
        // 创建临时节点
        result = client.create ().withMode ( CreateMode.EPHEMERAL ).forPath ( "/curator/ephemeral","/curator/ephemeral data".getBytes () );
        System.out.println ( result );
        // 连续创建临时序列化节点
        result = client.create ().withMode ( CreateMode.EPHEMERAL_SEQUENTIAL ).forPath ( "/curator/ephemeral_sequential","/curator/ephemeral_sequential data".getBytes () );
        System.out.println ( result );
        result = client.create ().withMode ( CreateMode.EPHEMERAL_SEQUENTIAL ).forPath ( "/curator/ephemeral_sequential","/curator/ephemeral_sequential data".getBytes () );
        System.out.println ( result );

        // withProtection 在创建节点名称之前添加GUID 标识，避免节点创建成功，但在返回之前出现异常，导致临时节点未被立即删除，而客户端也无法判定节点是否真正创建成功(创建临时节点未必一定成功，具有随机性)
        result = client.create ().withProtection ().withMode ( CreateMode.EPHEMERAL_SEQUENTIAL ).forPath (  "/curator/ephemeral_sequential","/curator/ephemeral_sequential data".getBytes () );
        System.out.println ( result );// /curator/_c_66cd7fbe-b612-402f-9ec8-3ce521cb0ae7-ephemeral_sequential0000000003
    }

    @Test
    public void testCheck() throws Exception{
        // 检查节点是否存在
        Stat stat1 = client.checkExists().forPath("/curator");
        Stat stat2 = client.checkExists().forPath("/curator2");

        System.out.println("'/curator'是否存在： " + (stat1 != null ? true : false));
        System.out.println("'/curator2'是否存在： " + (stat2 != null ? true : false));
    }

    @Test
    public void testGetAndSet() throws Exception{
        //获取某个节点的所有子节点
        System.out.println(client.getChildren().forPath("/"));

        //获取某个节点数据
        System.out.println(new String(client.getData().forPath("/curator")));

        //设置某个节点数据
        client.setData().forPath("/curator","/curator modified data1".getBytes());

        //获取某个节点数据
        System.out.println(new String(client.getData().forPath("/curator")));

    }

    @Test
    public void testSetDataAsync() throws Exception{
        //  后台操作触发监听
        //创建监听器
        CuratorListener listener = new CuratorListener() {

            @Override
            public void eventReceived(CuratorFramework client, CuratorEvent event)
                    throws Exception {
                System.out.println(event.getPath());
            }
        };

        //添加监听器
        client.getCuratorListenable().addListener(listener);

        //异步设置某个节点数据
        client.setData().inBackground().forPath("/curator","/curator modified data with Async".getBytes());

        //为了防止单元测试结束从而看不到异步执行结果，因此暂停10秒
        Thread.sleep(10000);

        /*
         *   null       启动时调用一次 eventReceived
         *   /curator   触发时调用一次 eventReceived
         *   null       关闭时调用一次 eventReceived
         */
    }

    @Test
    public void testSetDataAsyncWithCallback() throws Exception{
        //  后台操作触发监听，并且在启动 停机时都不触发监听回调函数
        BackgroundCallback callback = new BackgroundCallback() {

            @Override
            public void processResult(CuratorFramework client, CuratorEvent event)
                    throws Exception {
                System.out.println(event.getPath());
            }
        };

        //异步设置某个节点数据
        client.setData().inBackground(callback).forPath("/curator","/curator modified data with Callback".getBytes());

        System.out.println ("sleep for 10000 ms" ); // 先打印 sleep 再打印 path
        //为了防止单元测试结束从而看不到异步执行结果，因此暂停10秒
        Thread.sleep(10000);
    }

    @Test
    public void testDelete() throws Exception{
        //创建测试节点
        /*
            creatingParentContainersIfNeeded 级联创建
         */
        client.create().creatingParentContainersIfNeeded()
                .forPath("/curator/del_key1","/curator/del_key1 data".getBytes());

        client.create().creatingParentContainersIfNeeded()
                .forPath("/curator/del_key2","/curator/del_key2 data".getBytes());

        client.create().forPath("/curator/del_key2/test_key","test_key data".getBytes());


        //删除该节点
        client.delete().forPath("/curator/del_key1");

        List<String> children = client.getChildren ( ).forPath ( "/curator" );
        System.out.println ( children);


        //级联删除子节点
        client.delete().guaranteed().deletingChildrenIfNeeded().forPath("/curator/del_key2");
    }

    @Test
    public void testTransaction() throws Exception{
        //定义几个基本操作
        CuratorOp createOp = client.transactionOp().create()
                .forPath("/curator/one_path","some data".getBytes());

        CuratorOp setDataOp = client.transactionOp().setData()
                .forPath("/curator/one_path","other data".getBytes());

        CuratorOp deleteOp = client.transactionOp().delete()
                .forPath("/curator/one_path");

        //事务执行结果
        List<CuratorTransactionResult> results = client.transaction()
                .forOperations(createOp,setDataOp,deleteOp);

        //遍历输出结果
        for(CuratorTransactionResult result : results){
            System.out.println("执行结果是： " + result.getForPath() + "--" + result.getType());
        }
        /* 事务操作： 创建 修改 删除 打包成原子操作
            执行结果是： /curator/one_path--CREATE
            执行结果是： /curator/one_path--SET_DATA
            执行结果是： /curator/one_path--DELETE
         */
    }

    @Test
    public void testNamespace() throws Exception{
        //创建带命名空间的连接实例 withNameSpace = true
        // [zk: localhost:2181(CONNECTED) 4] ls /mydemo/v1/server1
        //[method1]
        client.create().creatingParentContainersIfNeeded()
                .forPath("/server1/method1","some data".getBytes());
        List<String> children = client.getChildren ( ).forPath ( "/" );
        System.out.println (children );
    }



}
