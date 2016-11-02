package ucb.oseproxy.rpc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;
import java.util.concurrent.atomic.AtomicBoolean;


import ucb.oseproxy.measure.MonitorTask;

public class BackgroundThread extends ClientThread {
  AtomicBoolean running = new AtomicBoolean(true);
  public BackgroundThread(String s1, int i1, String s2, String s3, String s4) {
    super(s1, i1, s2, s3, s4);
    // TODO Auto-generated constructor stub
  }
  
  private static final Logger logger = Logger.getLogger(ProxyClient.class.getName());
  public void stopLoop () {
    running.set(false);
  }
  @Override
  public void run() {
    
    try {
      String name = Thread.currentThread().getName(); 
      ProxyClient client = new ProxyClient("localhost", 50051);
      try {
        String connId = client.connect(dbURL, dbPort, dbname, username, password);
        // logger.info("Connection id " + connId);
        MonitorTask mt = new MonitorTask(1);
        
        while (running.get()) {
        // replace this with a query mix
          // ResultSet rs = client.selectRandomQuery();
         ResultSet rs = client.execQuery(connId, "select * from persons where personid="+ 1);
         rs.next();
         mt.add();
        }

      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } finally {
        client.shutdown();
      }
  }
  catch (InterruptedException e) {
      e.printStackTrace();
  }
  }
  

}
