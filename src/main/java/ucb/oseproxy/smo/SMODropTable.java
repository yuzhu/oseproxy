package ucb.oseproxy.smo;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class SMODropTable extends SMOSimpleCommand {
  String tableName;
  public SMODropTable() {
    // TODO Auto-generated constructor stub
  }

  public SMODropTable(String tablename) {
    this.cmd = Command.DROP_TABLE;
    this.tableName = tablename;
  }

  @Override
  public void executeSMO() {
    String dropTableString = "drop table if exists "  + tableName;
    try {
      Statement stmt = conn.createStatement(); 
      stmt.executeUpdate( (dropTableString));
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
