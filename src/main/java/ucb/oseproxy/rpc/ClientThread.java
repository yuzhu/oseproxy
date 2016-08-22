package ucb.oseproxy.rpc;

import java.util.logging.Logger;

import ucb.oseproxy.smo.SMOCommand.Command;

public class ClientThread extends Thread {
  protected String dbURL;
  protected int dbPort;
  protected String dbname;
  protected String username;
  protected String password;
  
  private static final Logger logger = Logger.getLogger(ProxyClient.class.getName());

  public ClientThread (String s1, int i1, String s2, String s3, String s4)  {
    dbURL = s1;
    dbPort = i1;
    dbname = s2;
    username = s3;
    password = s4;
    
  }
  @Override
  public void run() {
    try {
      String name = Thread.currentThread().getName(); 
      ProxyClient client = new ProxyClient("localhost", 50051);
      try {
        String connId = client.connect(dbURL, dbPort, dbname, username, password);
        logger.info("Thread " + name + " Connection id " + connId);
         
        String opt[] = {"largeppl", "personid", "lastname, firstname", "address,city"};
        // Issue SMO
        String smoId = client.issueSMO(connId, Command.DECOMPOSE_TABLE, opt);
        client.commitSMO(connId, smoId);
        
         
      } finally {
        client.shutdown();
      }
  }
  catch (InterruptedException e) {
      e.printStackTrace();
  }
  }
}
