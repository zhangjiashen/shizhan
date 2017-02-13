package cn.itcast.bigdata.zk;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import org.junit.Test.None;

public class SimpleZkClient {
	
	private static final String connectString = "192.168.80.3:2181,192.168.80.10:2181,192.168.80.11:2181";
	private static final int sessionTimeout = 30000;
	ZooKeeper zkClient = null;
	
	
	
	@Before
	public void init() throws IOException {
		zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				// TODO Auto-generated method stub
				System.out.println(event.getType() + event.getPath());
				
				try {
					zkClient.getChildren("/", true);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
			}
		});
		
	}
	public static void main(String[] args){
		
	}
	@Test
	public void testCreate() throws KeeperException, InterruptedException {
		String nodeCreated = zkClient.create("/ecplise", "hellozk".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		
	}
	
	@Test
	public void testExist() throws KeeperException, InterruptedException {
		Stat exists = zkClient.exists("/ecplise", false);
		System.out.println(exists == null ? "not exist":"exist");
	}
	@Test
	public void getChildren() throws Exception{
		
	
		List<String> children = zkClient.getChildren("/", true);
		
		for (String child : children) {
			System.out.println(child);
			System.out.println("helloworld");
		}
		
		Thread.sleep(Long.MAX_VALUE);
		
		
		
	}
	
	@Test
	public void getData() throws KeeperException, InterruptedException {
		byte[] data = zkClient.getData("/ecplise", false, null);
		
		System.out.println(new String(data));
	}
	
	@Test
	public void deleteData() throws KeeperException, InterruptedException {
		zkClient.delete("/ecplise", -1);
	}
	
	@Test
	public void setData() throws KeeperException, InterruptedException {
		zkClient.setData("/app1", "I miss you".getBytes(), -1);
		byte[] data = zkClient.getData("/app1", false, null);
		System.out.println(new String(data));
	}
}
