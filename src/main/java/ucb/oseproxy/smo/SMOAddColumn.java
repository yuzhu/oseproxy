package ucb.oseproxy.smo;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

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
    } else if(expr.equals("autoincrement()")){
      alterTableString = "ALTER TABLE %s ADD COLUMN %s SERIAL;";
      cmdString = String.format(alterTableString, this.tableName, this.columnName, this.expr);
    } else {
      alterTableString = "ALTER TABLE %s ADD COLUMN %s VARCHAR(30), ALTER COLUMN %s SET DEFAULT '%s'; ";
      // Assume String expr has double quotes around it, we convert it to single quotes
      String exp = this.expr.substring(1,this.expr.length()-1);
      cmdString = String.format(alterTableString, this.tableName, this.columnName, this.columnName, 
          exp);
    }
    
    try {
      Statement stmt = conn.createStatement(); 
      System.out.println("SQL STMT :" + cmdString);
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

