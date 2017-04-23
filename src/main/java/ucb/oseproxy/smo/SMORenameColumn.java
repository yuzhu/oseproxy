package ucb.oseproxy.smo;

import java.sql.SQLException;
import java.sql.Statement;

public class SMORenameColumn extends SMOSimpleCommand {
  private String columnFromName;
  private String columnToName;
  
  private String tableName;

  public SMORenameColumn(String string, String string2, String tableName) {
    this.columnFromName = string;
    this.columnToName = string2;
    this.tableName = tableName;
  }

  @Override
  public void executeSMO() {
    String alterTableString = "ALTER TABLE %s RENAME COLUMN %s TO %s;";
    String cmdString = String.format(alterTableString, this.tableName, this.columnFromName, this.columnToName);
    try {
      Statement stmt = conn.createStatement(); 
      stmt.executeUpdate((cmdString));
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

}
