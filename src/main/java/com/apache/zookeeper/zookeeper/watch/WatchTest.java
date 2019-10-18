package com.apache.zookeeper.zookeeper.watch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class WatchTest {

    private Log logger = LogFactory.getLog ( WatchTest.class );

    private ZooKeeper zkClient = null;

    private int timeout = 2000;

    private String connectString = "hadoop01:2181,hadoop02:2181,hadoop01:2181";

    private boolean alwaysAlert = false;

    @Before
    public void init() throws Exception {
        zkClient = new ZooKeeper ( connectString , timeout , new Watcher ( ) {
            @Override
            public void process ( WatchedEvent watchedEvent ) {
                if(alwaysAlert){
                    cycleRegisted(watchedEvent);
                }else{
                    noCycleRegisted(watchedEvent);
                }
            }
        } );
        logger.info ( "create zkClient success." );
    }

    @After
    public void close() throws InterruptedException {
        zkClient.close ();
    }

    @Test
    public void noWatch() throws KeeperException, InterruptedException {
        // alwaysAlert = false
        List<String> children = zkClient.getChildren ( "/servers" , false );
        logger.info ( String.format ( "get %s, then sleep..." ,children) );
        Thread.sleep ( Long.MAX_VALUE );
    }

    @Test
    public void onceWatch() throws KeeperException, InterruptedException {
        // alwaysAlert = false
        // getChildren watch 参数决定是否启用 watch 回调函数
        List<String> children = zkClient.getChildren ( "/servers" , true );
        logger.info ( String.format ( "get %s, then sleep..." ,children) );
        Thread.sleep ( Long.MAX_VALUE );
    }

    @Test
    public void alwaysWatch() throws KeeperException, InterruptedException {
        // alwaysAlert = true
        List<String> children = zkClient.getChildren ( "/servers" , true );
        logger.info ( String.format ( "get %s, then sleep..." ,children) );
        Thread.sleep ( Long.MAX_VALUE );
    }


    public void noCycleRegisted( WatchedEvent watchedEvent){
        logger.info ( watchedEvent.toString () );
    }

    public void cycleRegisted( WatchedEvent watchedEvent) {
        try {
            List<String> children = zkClient.getChildren ( "/servers" , true );
            logger.info ( watchedEvent.toString () );
        } catch (KeeperException e) {
            e.printStackTrace ( );
        } catch (InterruptedException e) {
            e.printStackTrace ( );
        }
    }







}
