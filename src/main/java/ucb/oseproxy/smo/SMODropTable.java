package ucb.oseproxy.smo;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class SMODropTable extends SMOSimpleCommand {
  private String tableName;
  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
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
