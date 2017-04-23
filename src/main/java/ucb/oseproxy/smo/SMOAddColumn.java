package ucb.oseproxy.smo;

import java.sql.SQLException;
import java.sql.Statement;

public class SMOAddColumn extends SMOSimpleCommand {
  private String columnName;
  private String tableName;
  private String expr;
  
  public SMOAddColumn(String colName, String expr, String tableName ) {
    this.columnName = colName;
    this.tableName = tableName;
    this.expr = expr;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getExpr() {
    return expr;
  }

  public void setExpr(String expr) {
    this.expr = expr;
  }

  @Override
  public void executeSMO() {
    String alterTableString;
    String cmdString;
    if (expr.isEmpty()){
      alterTableString = "ALTER TABLE %s ADD COLUMN %s VARCHAR(30);";
      cmdString = String.format(alterTableString, this.tableName, this.columnName);
    } else if(expr.equals("auto_increment()")){
      alterTableString = "ALTER TABLE %s ADD COLUMN %s SERIAL;";
      cmdString = String.format(alterTableString, this.tableName, this.columnName, this.expr);
    } else {
      alterTableString = "ALTER TABLE %s ADD COLUMN %s VARCHAR(30) SET DEFAULT %s;";
      cmdString = String.format(alterTableString, this.tableName, this.columnName, this.expr);
    }
    
    try {
      Statement stmt = conn.createStatement(); 
      stmt.executeUpdate((cmdString));
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
    String alterTableString = "ALTER TABLE %s DROP COLUMN %s;";
    String cmdString = String.format(alterTableString, this.tableName, this.columnName);
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

