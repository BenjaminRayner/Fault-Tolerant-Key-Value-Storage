import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.CuratorWatcher;
import java.net.InetSocketAddress;


public class KeyValueHandler implements KeyValueService.Iface 
{
  private Map<String, String> myMap = new ConcurrentHashMap<String, String>();
  private CuratorFramework curClient;
  private String zkNode;
  private String host;
  private int port;

  private boolean init = false;
  private Watcher watcher = new Watcher();
  private AtomicBoolean isPrimary = new AtomicBoolean(false);

  private int numThreads = 8;
  private AtomicBoolean backupRunning = new AtomicBoolean(false);
  private BlockingQueue<KeyValueService.Client> backupServerPool = new LinkedBlockingQueue<>(numThreads);

  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

  public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) throws Exception 
  {
    this.host = host;
    this.port = port;
    this.curClient = curClient;
    this.zkNode = zkNode;

    // Watch for changes in /brayner
    curClient.getChildren().usingWatcher(watcher).forPath(zkNode);
  }

  public String get(String key)
  {	
    String ret = myMap.get(key);
    if (ret == null)
        return "";
    else
        return ret;
  }

  public void put(String key, String value)
  {
    try {
      lock.readLock().lock();
      myMap.put(key, value);
      // Forward keys to backup if running
      if (isPrimary.get() && backupRunning.get()) {
        KeyValueService.Client backupClient = backupServerPool.take();
        backupClient.put(key, value);
        backupServerPool.add(backupClient);
      }
    } catch (Exception e) {
      // Backup crashed...
      backupRunning.set(false);
    }
    finally {
      lock.readLock().unlock();
    }
  }

  public void copy(Map<String,String> copy) {
    myMap.putAll(copy);
  }

  void isPrimary() throws Exception 
  {
    curClient.sync();
    List<String> children = curClient.getChildren().usingWatcher(watcher).forPath(zkNode);
    Collections.sort(children);
    byte[] data = curClient.getData().forPath(zkNode + "/" + children.get(0));
    String strData = new String(data);
    String[] primaryAddress = strData.split(":");
    isPrimary.set((primaryAddress[0].equals(host)) && (primaryAddress[1].equals(String.valueOf(port))));
  }
  InetSocketAddress getBackup() throws Exception 
  {
    curClient.sync();
    List<String> children = curClient.getChildren().usingWatcher(watcher).forPath(zkNode);
    // Backup already crashed
    if (children.size() != 2) {
      return null;
    }
    Collections.sort(children);
    byte[] data = curClient.getData().forPath(zkNode + "/" + children.get(1));
    String strData = new String(data);
    String[] primaryAddress = strData.split(":");
    return new InetSocketAddress(primaryAddress[0], Integer.parseInt(primaryAddress[1]));
  }

  class Watcher implements CuratorWatcher
  {
    synchronized public void process(WatchedEvent event) throws Exception 
    {
      // Own zNode added. Check if primary
      if (init == false) { init = true; isPrimary(); return; }

      InetSocketAddress backupAddress = getBackup();

      // Backup added/crashed (Primary enter)
      if (isPrimary.get()) {
        if (backupAddress == null) return;  // Backup crashed (zNode gone)

        // Connect to backup thrift server with 8 clients
        KeyValueService.Client[] tempBuf = new KeyValueService.Client[numThreads];
        try {
          for (int i = 0; i < numThreads; ++i) {
            TSocket sock = new TSocket(backupAddress.getHostName(), backupAddress.getPort());
            TTransport transport = new TFramedTransport(sock);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            tempBuf[i] = new KeyValueService.Client(protocol);
          }
        }
        // Backup crashed...
        catch (Exception e) { return; }

        // Setup backup server
        try {
          lock.writeLock().lock();
          // Add clients to pool
          backupServerPool.clear();
          for (int i = 0; i < numThreads; ++i) {
            backupServerPool.add(tempBuf[i]);
          }
          // Copy k/v pairs over
          KeyValueService.Client backupClient = tempBuf[0];
          backupClient.copy(myMap);
          backupRunning.set(true);
        }
        catch (Exception e) {}
        finally {
          lock.writeLock().unlock();
        }
      }
      // Primary crash (Backup enter)
      else isPrimary.set(true);
    }
  }
}

