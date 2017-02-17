package ucb.oseproxy.smo;

import java.sql.SQLException;
import java.sql.Statement;

public class SMORenameTable extends SMOSimpleCommand {
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

  public SMORenameTable(String fromName, String toName) {
    this.fromName = fromName;
    this.toName = toName;
  }

  @Override
  public void executeSMO() {
    String alterTableString = "ALTER TABLE %s RENAME TO %s;";
    String cmdString = String.format(alterTableString, fromName, toName);
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
    return true;
  }

  @Override
  public boolean rollbackSMO() {
    // TODO Auto-generated method stub
    String alterTableString = "ALTER TABLE %s RENAME TO %s;";
    String cmdString = String.format(alterTableString, toName, fromName);
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
