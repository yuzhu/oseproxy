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

}
