package ucb.oseproxy.smo;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class SMODropTable extends SMOSimpleCommand {
  private String tableName;
  private String hiddenTableName;
  public static final String HIDDEN_PREFIX = "OSEHIDDEN__";
  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public SMODropTable(String tablename) {
    this.cmd = Command.DROP_TABLE;
    this.tableName = tablename;
    this.hiddenTableName = HIDDEN_PREFIX + tableName;
  }

   // This renames the table to a hidden name
  @Override
  public void executeSMO() {
    
    String renameTableString = "ALTER TABLE %s RENAME TO %s ";

    String cmdString = String.format(renameTableString, this.tableName, this.hiddenTableName);
    try {
      Statement stmt = conn.createStatement(); 
      stmt.executeUpdate(cmdString);
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  // this drops the hidden table from the database, officially committing the change
  @Override
  public boolean commitSMO() {
    // TODO Auto-generated method stub
    String dropTableString = "drop table if exists "  + hiddenTableName;
    
    try {
      Statement stmt = conn.createStatement(); 
      stmt.executeUpdate( (dropTableString));
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return true;
  }

  @Override
  public boolean rollbackSMO() {
    // TODO Auto-generated method stub
    String renameTableString = "ALTER TABLE %s RENAME TO %s ";

    String cmdString = String.format(renameTableString,this.hiddenTableName,  this.tableName);
    try {
      Statement stmt = conn.createStatement(); 
      stmt.executeUpdate(cmdString);
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return true;
  }

  @Override
  public boolean isReversible() {
    // TODO Auto-generated method stub
    return true;
  }
}
