package com.renren.infra.xzl;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class TestLock implements Runnable {

	private static Logger logger = LogManager.getLogger(TestLock.class
			.getName());

	private ZooKeeper zk;
	private String connectString;
	private int sessionTimeout;
	private CountDownLatch connectedSignal;
	private String path;
	private String node;
	private long nodeId;
	private String preNode;
	private CountDownLatch lockSingal;
	private Runnable runnable;

	public TestLock(String connectString, int sessionTimeout, String prefix,
			String lockName) {
		this(connectString, sessionTimeout, prefix, lockName, null);
	}

	public TestLock(String connectString, int sessionTimeout, String prefix,
			String lockName, Runnable runnable) {
		this.connectString = connectString;
		this.sessionTimeout = sessionTimeout;
		this.path = prefix + "/" + lockName;
		this.runnable = runnable;
	}

	public void init() {
		try {
			createConnection();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void close() {
		try {
			releaseConnecion();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public Runnable getRunnable() {
		return runnable;
	}

	public void setRunnable(Runnable runnable) {
		this.runnable = runnable;
	}

	public void run() {
		try {
			lock();
			if (runnable != null) {
				runnable.run();
			}
			unlock();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void createConnection() throws InterruptedException, IOException {
		releaseConnecion();
		connectedSignal = new CountDownLatch(1);
		zk = new ZooKeeper(connectString, sessionTimeout, new SessionWatcher());
		connectedSignal.await();
	}

	private void releaseConnecion() throws InterruptedException {
		if (zk != null) {
			zk.close();
		}
	}

	private void lock() throws KeeperException, InterruptedException {
		node = zk.create(path + "/" + zk.getSessionId() + "-", null,
				Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		logger.info("I am " + node + ", want to get lock.");
		nodeId = getId(node);
		lockSingal = new CountDownLatch(1);
		checkState();
		lockSingal.await();
	}

	private void unlock() throws InterruptedException, KeeperException {
		zk.delete(node, -1);
		logger.info("I am " + node + ", finished my work, unlock!");
	}

	private void checkState() {
		try {
			if (isMinNodeId()) {
				lockSingal.countDown();
				logger.info("I am " + node + ", the min node, lock!");
			} else if (zk.exists(preNode, new NodeWatcher()) == null) {
				logger.info("I am " + node + ", " + preNode
						+ " was deleted before watch, check state again.");
				checkState();
			} else {
				logger.info("I am " + node + ", isn`t the min node, watch "
						+ preNode + ".");
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private boolean isMinNodeId() throws KeeperException, InterruptedException {
		long minNodeId = Long.MAX_VALUE;
		long maxNodeId = Long.MIN_VALUE;
		List<String> children = zk.getChildren(path, false);
		for (String child : children) {
			long id = getId(child);
			if (id < nodeId && id > maxNodeId) {
				maxNodeId = id;
				preNode = path + "/" + child;
			}
			minNodeId = Math.min(minNodeId, id);
		}
		return nodeId == minNodeId;
	}

	private long getId(String node) {
		return Long.parseLong(node.substring(node.lastIndexOf("-") + 1,
				node.length()));
	}

	private class SessionWatcher implements Watcher {

		public void process(WatchedEvent event) {
			switch (event.getState()) {
			case SyncConnected:
				connectedSignal.countDown();
				logger.info("Successfully connected to ZK.");
				break;
			case Expired:
				logger.error("Detected conn to zk session expired. retry conn");
				try {
					createConnection();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				break;
			case Disconnected:
				logger.error("Lost connection to ZK. Disconnected, will auto reconnected until success.");
				break;
			default:
				break;
			}
		}
	}

	private class NodeWatcher implements Watcher {

		public void process(WatchedEvent event) {
			if (event.getType().equals(EventType.NodeDeleted)
					&& event.getPath().equals(preNode)) {
				logger.info("I am node " + node
						+ ", received watch event, want to lock.");
				checkState();
			}
		}

	}
}
