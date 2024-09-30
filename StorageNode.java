import java.io.*;
import java.util.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.utils.*;

import org.apache.log4j.*;

public class StorageNode 
{
    static Logger log;

  public static void main(String [] args) throws Exception 
  {
    BasicConfigurator.configure();
    log = Logger.getLogger(StorageNode.class.getName());

    if (args.length != 4) {
        System.err.println("Usage: java StorageNode host port zkconnectstring zknode");
        System.exit(-1);
    }

    CuratorFramework curClient =
        CuratorFrameworkFactory.builder()
        .connectString(args[2])
        .retryPolicy(new RetryNTimes(10, 1000))
        .connectionTimeoutMs(1000)
        .sessionTimeoutMs(10000)
        .build();

    curClient.start();

    // End communication with zookeeper when process is shutdown
    // Will remove the emepheral znode created by this server
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
          curClient.close();
      }
    });

    // Setup multithreaded thrift server on specific host:port
    // Handles get() and put() requests from client
    KeyValueService.Processor<KeyValueService.Iface> processor = new KeyValueService.Processor<>(new KeyValueHandler(args[0], Integer.parseInt(args[1]), curClient, args[3]));
    TServerSocket socket = new TServerSocket(Integer.parseInt(args[1]));
    TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
    sargs.protocolFactory(new TBinaryProtocol.Factory());
    sargs.transportFactory(new TFramedTransport.Factory(999999999));
    sargs.processorFactory(new TProcessorFactory(processor));
    sargs.maxWorkerThreads(64);
    TServer server = new TThreadPoolServer(sargs);
    log.info("Launching server");

    new Thread(new Runnable() {
      public void run() {
          server.serve();
      }
    }).start();

    // Wait for any crashed zNodes to be cleaned up
    while (curClient.getChildren().forPath(args[3]).size() > 2) {
      Thread.sleep(100);
    }

    // Add this server's zNode
    curClient.create()
    .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)                           // zNode's name will be increasing and be deleted on curClient.close()
    .forPath(args[3] + "/znode", (args[0] + ":" + args[1]).getBytes());  // zNode is created in /brayner/znodeXXXXX with data: "host:port"
  }
}
