package ucb.oseproxy.rpc;

import java.util.logging.Logger;

import ucb.oseproxy.smo.SMOCommand.Command;

public class SMOThread extends ClientThread {

  public SMOThread(String s1, int i1, String s2, String s3, String s4) {
    super(s1, i1, s2, s3, s4);
    // TODO Auto-generated constructor stub
  }

  private static final Logger logger = Logger.getLogger(ProxyClient.class.getName());

  @Override
  public void run() {
    try {
      String name = Thread.currentThread().getName(); 
      ProxyClient client = new ProxyClient("localhost", 50051);
      try {
        String connId = client.connect(dbURL, dbPort, dbname, username, password);
        logger.info("Thread " + name + " Connection id " + connId);
         
        String opt[] = {"largeppl", "personid", "lastname,firstname", "address,city"};
        // Issue SMO
        client.issueSMO(connId, Command.DECOMPOSE_TABLE, opt);
         
      } finally {
        client.shutdown();
      }
  }
  catch (InterruptedException e) {
      e.printStackTrace();
  }
  }
}
