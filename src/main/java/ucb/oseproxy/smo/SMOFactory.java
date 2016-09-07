package ucb.oseproxy.smo;

import java.sql.Connection;

import java.util.List;

import ucb.oseproxy.smo.SMOCommand.Command;
import ucb.oseproxy.smo.SMOParser;


public class SMOFactory {
  
  public static SMOCommand getSMO(String cmdLine) {
    //SmoParser sParser = new SmoParser();
    return null;
  }
  public static SMOCommand getSMO(Connection conn, Command cmd,  List<String> options) {
    switch (cmd) {
      case DECOMPOSE_TABLE:
        return new SMODecompose(conn, cmd, options);
        
      case MERGE_TABLE:
        return new SMOMerge(conn, cmd, options);
        
      default:
        return null;
    }
  }
}
