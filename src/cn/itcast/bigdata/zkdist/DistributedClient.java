package cn.itcast.bigdata.zkdist;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class DistributedClient {
	
	
	private static final String connectString = "192.168.80.3:2181,192.168.80.10:2181,192.168.80.11:2181";
	private static final int sessionTimeout = 30000;
	private static final String parentNode = "/servers";
	private volatile List<String> serverList = null; 
	ZooKeeper zk = null;
	
	public void getConnect() throws IOException {
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				// TODO Auto-generated method stub
				//System.out.println(event.getType() + event.getPath());
				
				try {
					getServerList();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
			}
		});
	}
	
	public void getServerList() throws KeeperException, InterruptedException{
		List<String> children = zk.getChildren(parentNode, true);
		List<String> servers = new ArrayList<String>();
		for (String child : children) {
			byte[] server = zk.getData(parentNode+"/"+child, false, null);
			servers.add(new String(server));
		}
		serverList = servers;
		
		System.out.println(serverList);
	}
	
	public void handleBusiness() throws InterruptedException {
		System.out.println("client start working...");
		Thread.sleep(Long.MAX_VALUE);
		
	}
	
	
	public static void main(String[] args) throws Exception {
		
		DistributedClient client = new DistributedClient();
		
		client.getConnect();
		
		client.getServerList();
		client.handleBusiness();
		
		
	}

}
