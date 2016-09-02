package ucb.oseproxy.smo;

import java.sql.Connection;
import java.util.List;

import ucb.oseproxy.smo.SMOCommand.Command;

public class SMOFactory {
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
