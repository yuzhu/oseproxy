package ucb.oseproxy.smo;

import java.sql.SQLException;
import java.sql.Statement;

public class SMOCopyTable extends SMOSimpleCommand {
  private String fromName, toName;
  public String getFromName() {
    return fromName;
  }

  public void setFromName(String fromName) {
    this.fromName = fromName;
  }

  public String getToName() {
    return toName;
  }

  public void setToName(String toName) {
    this.toName = toName;
  }

  public SMOCopyTable(String fromName, String toName) {
    this.fromName = fromName;
    this.toName = toName;
  }

  @Override
  public void executeSMO() {
    String createTableString = "CREATE TABLE %s AS SELECT * FROM %s;";
    String cmdString = String.format(createTableString, this.fromName, this.toName);
    try {
      Statement stmt = conn.createStatement(); 
      stmt.executeUpdate( (cmdString));
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public boolean commitSMO() {
    // TODO Auto-generated method stub
    return super.commitSMO();
  }

  @Override
  public boolean rollbackSMO() {
    // TODO Auto-generated method stub
    String dropTableString = "DROP TABLE %s;";
    String cmdString = String.format(dropTableString, this.toName);
    try {
      Statement stmt = conn.createStatement(); 
      stmt.executeUpdate( (cmdString));
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
