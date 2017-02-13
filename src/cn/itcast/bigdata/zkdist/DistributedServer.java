package cn.itcast.bigdata.zkdist;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class DistributedServer {
	
	
	private static final String connectString = "192.168.80.3:2181,192.168.80.10:2181,192.168.80.11:2181";
	private static final int sessionTimeout = 30000;
	private static final String parentNode = "/servers";
	ZooKeeper zk = null;
	
	public void getConnect() throws IOException {
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				// TODO Auto-generated method stub
				System.out.println(event.getType() + event.getPath());
				
				try {
					zk.getChildren("/", true);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
			}
		});
		
		
	}
	
	public void registerServer(String hostname) throws KeeperException, InterruptedException {
		
		String create = zk.create(parentNode + "/server", hostname.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(hostname + "is created" +create);
		
	}
	
	public void handleBusiness(String hostname) throws InterruptedException {
		System.out.println(hostname + "start working...");
		Thread.sleep(Long.MAX_VALUE);
		
	}
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		DistributedServer server = new DistributedServer();
		server.getConnect();
		
		server.registerServer(args[0]);
		server.handleBusiness(args[0]);
	}

}
