import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.*;

import org.apache.log4j.*;

import ca.uwaterloo.watca.ExecutionLogger;


public class A3Client implements CuratorWatcher 
{
  static Logger log;
  
  String zkConnectString;
  String zkNode;
  int numThreads;
  int numSeconds;
  int keySpaceSize;
  CuratorFramework curClient;
  volatile boolean done = false;
  AtomicInteger globalNumOps;
  volatile InetSocketAddress primaryAddress;
  ExecutionLogger exlog;

  public static void main(String [] args) throws Exception 
  {
    if (args.length != 5) {
      System.err.println("Usage: java A3Client zkconnectstring zknode num_threads num_seconds keyspace_size");
      System.exit(-1);
    }

    BasicConfigurator.configure();
    log = Logger.getLogger(A3Client.class.getName());

    A3Client client = new A3Client(args[0], args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]));

    try {
      client.start();
      client.execute();
    } 
    catch (Exception e) {
      log.error("Uncaught exception", e);
    } 
    finally {
      client.stop();
    }
  }

  A3Client(String zkConnectString, String zkNode, int numThreads, int numSeconds, int keySpaceSize) 
  {
		this.zkConnectString = zkConnectString; 
		this.zkNode = zkNode;
		this.numThreads = numThreads;
		this.numSeconds = numSeconds;
		this.keySpaceSize = keySpaceSize;
		globalNumOps = new AtomicInteger();
		primaryAddress = null;
		exlog = new ExecutionLogger("execution.log");
	}

  // Start up the curator client used to interface with the zookeeper server (127.0.0.1:2181)
  // curClient constantly pings heartbeats to zookeeper
	void start() 
  {
		curClient =
			CuratorFrameworkFactory.builder()
			.connectString(zkConnectString)         // Set the url to the zookeeper server (in our case, 127.0.0.1:2181)
			.retryPolicy(new RetryNTimes(10, 1000)) // Retry 10 times, wait 1s between each retry.
			.connectionTimeoutMs(1000)              // Set connection timeout to 1s.
			.sessionTimeoutMs(10000)                // Terminates the session if zookeeper does not hear from the client for more than 10s
			.build();

		curClient.start();
		exlog.start();
	}

  // Exchange k/v pairs with primary server and measure runtime
	void execute() throws Exception 
  {
    // Make sure to get latest primary server
		primaryAddress = getPrimary();

    // Spin up threads with thrift clients that exchange k/v pairs with primary
    List<Thread> tlist = new ArrayList<>();
		List<MyRunnable> rlist = new ArrayList<>();
		for (int i = 0; i < numThreads; i++) {
			MyRunnable r = new MyRunnable(); // Each myRunnable has a thrift client initialized to connect to primary
			Thread t = new Thread(r);
			tlist.add(t);
			rlist.add(r);
		}
		long startTime = System.currentTimeMillis();
		for (int i = 0; i < numThreads; i++) {
			tlist.get(i).start();
		}
		log.info("Done starting " + numThreads + " threads...");
		System.out.println("Done starting " + numThreads + " threads...");

    // After a delay, done = true
    // Will cause active MyRunnable threads to return
		Thread.sleep(numSeconds * 1000);
		done = true; 

    // Wait for threads to return
		for (Thread t: tlist) {
			t.join(1000);
		}
    
    // Add up runtime of threads
		long estimatedTime = System.currentTimeMillis() - startTime;
		int tput = (int)(1000f * globalNumOps.get() / estimatedTime);
		System.out.println("Aggregate throughput: " + tput + " RPCs/s");
		long totalLatency = 0;
		for (MyRunnable r: rlist) {
			totalLatency += r.getTotalTime();
		}
		double avgLatency = (double)totalLatency / globalNumOps.get() / 1000;
		System.out.println("Average latency: " + ((int)(avgLatency*100))/100f + " ms");
	}

  // End communication with zookeeper
	void stop() 
  {
		curClient.close();
		exlog.stop();
  }

  // Get the latest primary znode
  InetSocketAddress getPrimary() throws Exception 
  {
    while (true) {
      curClient.sync();         // sync the zookeeper cluster to make sure the client will get the newest data
      List<String> children = 
        curClient.getChildren() // get the children list
        .usingWatcher(this)     // set a watcher to listen to changes
        .forPath(zkNode);       // of a given directory (ZNode) (/brayner)
      if (children.size() == 0) {
        log.error("No primary found");
        Thread.sleep(100);
        continue;
      }

      // Primary is determined by getting the first znode in lexicographically ordered list
      Collections.sort(children);
      byte[] data = curClient.getData().forPath(zkNode + "/" + children.get(0));

      // Get data of znode (IP:PORT)
      String strData = new String(data);
      String[] primary = strData.split(":");
      log.info("Found primary " + strData);

      // Return a socket using the znode's data
      return new InetSocketAddress(primary[0], Integer.parseInt(primary[1]));
    }
  }

  // Returns a thrift client that connects to the KeyValueService on the current primary
  KeyValueService.Client getThriftClient() 
  {
    while (true) {
      try {
        TSocket sock = new TSocket(primaryAddress.getHostName(), primaryAddress.getPort());
        TTransport transport = new TFramedTransport(sock);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        return new KeyValueService.Client(protocol);
      } 
      catch (Exception e) {
        log.error("Unable to connect to primary");
      }

      try {
        Thread.sleep(100);
      } 
      catch (InterruptedException e) {}
    }
  }

  // When watcher of /brayner detects primary change, determine new primary
  synchronized public void process(WatchedEvent event) 
  {
    log.info("ZooKeeper event " + event);
    try {
      primaryAddress = getPrimary();
    } 
    catch (Exception e) {
      log.error("Unable to determine primary");
    }		
  }

  // Utilizes the thrift client to exhange k/v pairs with primary server
  // Up to numThreads instances may be running at once.
  class MyRunnable implements Runnable 
  {
    long totalTime;
    KeyValueService.Client client;
    MyRunnable() throws TException {
      client = getThriftClient();
    }
		
    long getTotalTime() { return totalTime; }
      
    public void run() {
      Random rand = new Random();
      totalTime = 0;
      long tid = Thread.currentThread().getId();
      int numOps = 0;
      try {
        while (!done) {
          long startTime = System.nanoTime();
          //log.info("Starting operation at : " + startTime);

          // Half the time will add k/v pair to primary
          // Half the time will get k/v from primary
          if (rand.nextBoolean()) {
            while (!done) {
              try {
                String key = "key-" + (Math.abs(rand.nextLong()) % keySpaceSize);
                String value = "value-" + Math.abs(rand.nextLong());
                exlog.logWriteInvocation(tid, key, value);
                client.put(key, value);
                exlog.logWriteResponse(tid, key);
                numOps++;
                break;
              }
              catch (Exception e) {
                // Primary no longer active. Update thrift client with new primary
                log.error("Exception during put");
                Thread.sleep(100);
                client = getThriftClient();
              }
            }
          } 
          else {
            while (!done) {
              try {
                String key = "key-" + (Math.abs(rand.nextLong()) % keySpaceSize);
                exlog.logReadInvocation(tid, key);
                String resp = client.get(key);
                exlog.logReadResponse(tid, key, resp);
                numOps++;
                break;
              } 
              catch (Exception e) {
                log.error("Exception during get");
                Thread.sleep(100);
                client = getThriftClient();
              }
            }
          }
          long diffTime = System.nanoTime() - startTime;
          totalTime += diffTime / 1000;
        }
      } 
      catch (Exception x) {
        x.printStackTrace();
      }	
      globalNumOps.addAndGet(numOps);
    }
  }
}
