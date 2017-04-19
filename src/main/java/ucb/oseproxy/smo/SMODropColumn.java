package ucb.oseproxy.smo;

import java.sql.SQLException;
import java.sql.Statement;

public class SMODropColumn extends SMOSimpleCommand {
  
  private String columnName;
  private String tableName;
  
  public SMODropColumn(String colName, String tableName) {
    this.columnName = colName;
    this.tableName = tableName;
  }

  @Override
  public void executeSMO() {
    String alterTableString = "ALTER TABLE %s DROP COLUMN %s;";
    String cmdString = String.format(alterTableString, this.tableName, this.columnName);
    try {
      Statement stmt = conn.createStatement(); 
      stmt.executeUpdate((cmdString));
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public boolean isReversible() {
    // TODO Auto-generated method stub
    return false;
  }
  
}
