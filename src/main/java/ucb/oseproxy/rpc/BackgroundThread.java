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
  private AtomicBoolean running = new AtomicBoolean(true);
  private int loadGenOpt = 0;
  public static final int READ_WRITE = 0;
  public static final int READ_MOSTLY = 1;
  public BackgroundThread(String s1, int i1, String s2, String s3, String s4) {
    super(s1, i1, s2, s3, s4);
    // TODO Auto-generated constructor stub
  }
  
  public BackgroundThread(String s1, int i1, String s2, String s3, String s4, int loadgenOpt) {
    super(s1, i1, s2, s3, s4);
    this.loadGenOpt = loadgenOpt;
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
        logger.info("loadGenOpt " + this.loadGenOpt);
        MonitorTask mt = new MonitorTask(1);
        int selector = 0;
        while (running.get()) {
        // replace this with a query mix
          // ResultSet rs = client.selectRandomQuery();
          ResultSet rs = null;
         if (this.loadGenOpt == READ_WRITE ) {
           if (selector %2 ==0)
            rs = client.execQuery(connId, "select * from semilargeppl where personid="+ selector);
          else 
           client.execUpdate(connId,  "insert into semilargeppl(lastname, firstname, address, city)  values('Brewer', 'Eric', 'Soda Hall', 'Berkeley');");
         } else //loadGenOpt == READ_MOSTLY
         {
          if (selector %10 ==0)
            client.execUpdate(connId,  "insert into semilargeppl(lastname, firstname, address, city)  values('Brewer', 'Eric', 'Soda Hall', 'Berkeley');");
          else 
            rs = client.execQuery(connId, "select * from semilargeppl where personid="+ selector);
            
         }
         if (rs != null) rs.next();
         mt.add();
         selector++;
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
