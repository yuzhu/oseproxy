package ucb.oseproxy.smo;

import java.sql.SQLException;
import java.sql.Statement;

public class SMOCopyColumn extends SMOSimpleCommand {

  private String fromTable;
  private String toTable;
  private String newColName;
  private String cond;
  
  
  public SMOCopyColumn(String colName, String fromTable, String toTable, String cond) {
    this.newColName = colName;
    this.fromTable = fromTable;
    this.toTable = toTable;
    this.cond = cond;
  }
  
  private boolean moveIntoEmptyTable() {
    return cond == null || cond.isEmpty();
    
  }

  @Override
  public void executeSMO() {
    
    String alterTableString;
    String cmdString;
    
      
    
    if (moveIntoEmptyTable()) {
      String dropTableString = "DROP TABLE " + this.toTable + ";";
      
      String createTableString = "CREATE TABLE %s AS SELECT %s FROM %s;";
      cmdString = String.format(createTableString, this.toTable, this.newColName, this.fromTable);
      try {
        Statement stmt = conn.createStatement(); 
        stmt.executeUpdate(dropTableString);
        
        stmt.executeUpdate((cmdString));
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      
    } else {
      alterTableString = "ALTER TABLE %s ADD COLUMN %s VARCHAR(30);";
      cmdString = String.format(alterTableString, this.toTable, this.newColName);
      String insertColString = "update %s set %s = (select %s from %s where %s);";
      String cmd2String =  String.format(insertColString, this.toTable, this.newColName, this.newColName,
          this.fromTable, this.cond);
      try {
        Statement stmt = conn.createStatement(); 
        // System.out.println(cmdString);
        // System.out.println(cmd2String);

        stmt.executeUpdate((cmdString));
        stmt.executeUpdate((cmd2String));
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      
    }

  }

}
