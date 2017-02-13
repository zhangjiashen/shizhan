package cn.itcast.bigdata.zklock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.theories.Theories;
import org.apache.zookeeper.ZooDefs.Ids;
import org.jboss.netty.bootstrap.ClientBootstrap;

public class DistributedClientLock {
	
	private static final String connectString = "192.168.80.3:2181,192.168.80.10:2181,192.168.80.11:2181";
	private static final int sessionTimeout = 30000;
	private static final String LockParentNode = "/lock";
	
	public volatile String myPath;
	ZooKeeper zk = null;
	
	@Before
	public void getConnect() throws IOException {
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				// TODO Auto-generated method stub
				//System.out.println(event.getType() + event.getPath());
				
				try {
				
					if(event.getType() == EventType.NodeChildrenChanged && event.getPath().equals(LockParentNode)){
						if(getLock() == null)
							System.out.println("not app...");
						else{
							if(isGetLock(getLock())){
								doSomething(getLock());
								registerLock();
							}
							else
								System.out.println("not get lock...");
						}
					}	
					
					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
			}
		});
	}
	
	@Test
	public void registerLock() throws KeeperException, InterruptedException {
		
		String create = zk.create(LockParentNode + "/app", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(create +" is created... ");
		myPath = create;
		
	}
	
	@Test
	public String getLock() throws KeeperException, InterruptedException{
		List<String> apps = zk.getChildren(LockParentNode, true);
		Collections.sort(apps);
		System.out.println(apps);
		if(apps.size() > 0)
			return LockParentNode+ "/" + apps.get(0);
		else
			return null;
	}
	
	public boolean isGetLock(String firstApp) {	
		
		
		System.out.println("myPath = " + this.myPath +" firstApp= "+ firstApp);
		if(myPath.equals(firstApp))
			return true;
		else
			return false;	
	}
	
	public void doSomething(String app) throws Exception, KeeperException {
		// TODO Auto-generated method stub
		System.out.println(app + " is doSomeing...");
		Thread.sleep(2000);
		zk.delete(app, -1);
		System.out.println(app + " is finished...");
	}
	
	public static void main(String[] args) throws Exception {
		
		DistributedClientLock client = new DistributedClientLock();
		client.getConnect();
		client.registerLock();
		
		if(client.isGetLock(client.getLock())){
			client.doSomething(client.getLock());
			client.registerLock();
		}
		Thread.sleep(Long.MAX_VALUE);
		
		
		
	}




}
