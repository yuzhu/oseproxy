package ucb.oseproxy.rpc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Scanner;
import java.util.logging.Logger;

import ucb.oseproxy.smo.SMOCommand.Command;

public class SMOThread extends ClientThread {
  private InputStream cmds;
  private BufferedReader reader;
  
  public SMOThread(String s1, int i1, String s2, String s3, String s4){
    super(s1, i1, s2, s3, s4);
    cmds = System.in;
    reader = new BufferedReader(new InputStreamReader(cmds));
    // TODO Auto-generated constructor stub
  }
  
  public SMOThread(String s1, int i1, String s2, String s3, String s4, String filename) throws FileNotFoundException {
    super(s1, i1, s2, s3, s4);
    cmds = new FileInputStream(new File(filename));
    reader = new BufferedReader(new InputStreamReader(cmds));
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
        // client.issueSMO(connId, Command.DECOMPOSE_TABLE, opt);
        //String cmd = "JOIN TABLE persons, calllog INTO calllogwithcity WHERE persons.personid=calllog.from_id;";
        // client.issueSMOString(connId, cmd);
        // String cmd2 = "JOIN TABLE persons, calllogwithcity INTO calllogwithcity2 WHERE persons.personid=calllogwithcity.to_id;";
        
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
          System.out.println("command is " + line);
          System.out.println("smo started");
          
          if (!(line.trim().isEmpty() || line.trim().equalsIgnoreCase(("NOP;"))))
            client.issueSMOString(connId, line);
          System.out.println("smo finished");
          
          Thread.sleep(300);
        }

        //client.issueSMOString(connId, cmd);
         
      } catch (Exception e) {
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
