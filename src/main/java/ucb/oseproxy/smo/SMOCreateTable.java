package ucb.oseproxy.smo;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class SMOCreateTable extends SMOSimpleCommand {
  private String tableName;
  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public List<String> getColumnList() {
    return columnList;
  }

  public void setColumnList(List<String> columnList) {
    this.columnList = columnList;
  }
  private List<String> columnList;
  public SMOCreateTable(String tableName, String collist) {
    this.tableName = tableName;
    columnList = Arrays.asList(collist.split(","));
  }

  public SMOCreateTable(String tableName, List<String> collist) {
    this.tableName = tableName;
    columnList = collist;
  }
  
  private String collist() {
    StringBuffer sb = new StringBuffer();
    for (String col : columnList) {
      sb.append(col);
      sb.append(",");
    }
    
    if (columnList.size() >= 1) {
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
  }
  
  private String typedCollist() {
    if (columnList.size() == 0)
      return "";
    StringBuffer sb = new StringBuffer();
    for (String col : columnList) {
      sb.append(col);
      sb.append(" VARCHAR(30)");
      sb.append(",");
    }
    
    if (columnList.size() >= 1) {
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
  }
  @Override
  public void executeSMO() {
    String createTableString = "CREATE TABLE %s (%s);";
    String cmdString = String.format(createTableString, tableName, typedCollist());
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
    String cmdString = String.format(dropTableString, this.tableName);
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
