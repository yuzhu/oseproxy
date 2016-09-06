package ucb.oseproxy.smo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import ucb.oseproxy.smo.SMOCommand.Command;
import java.lang.IllegalArgumentException;


// MERGE TABLE (R,S) INTO T, assuming R, S has the same structure
public class SMOMerge extends SMOAbstractCommand {

  private static Logger logger = Logger.getLogger(SMOAbstractCommand.class.getName());
  private String r,s,t;
  private List<String> tables, views;
  private List<String> fields;
  
  public SMOMerge(Connection conn, Command cmd2, List<String> options) {
    super(conn, cmd2, options);
    if (args.size() != 3) {
      logger.warning("Number of options not valid for DECOMPOSE_TABLE");
    }
    r = args.get(0);
    s = args.get(1);
    t = args.get(2);
    tables = new ArrayList<String>();
    views = new ArrayList<String>();
    tables.add(r);
    tables.add(s);
    views.add(t);
    
    Statement stmt;
    // populate fields list
    fields = new ArrayList<String>();
    try {
      stmt = conn.createStatement();
      // Verify R and S has same columns so they can merge
      ResultSet rsr = stmt.executeQuery("select * from " + r + " where 0=1");
      ResultSetMetaData rsmdr = rsr.getMetaData();
      
      ResultSet rst = stmt.executeQuery("select * from " + s + " where 0=1");
      ResultSetMetaData rsmdt = rst.getMetaData();
      if (rsmdr.getColumnCount() != rsmdt.getColumnCount())
        throw new IllegalArgumentException("column mismatch");
      for (int i = 1; i<= rsmdr.getColumnCount(); i++){
        if (!rsmdr.getColumnName(i).equals( rsmdt.getColumnName(i)))
          throw new IllegalArgumentException("column mismatch");
        fields.add(rsmdr.getColumnName(i));
      }
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
  }

  @Override
  protected List<String> getTables() {
    return tables;
  }

  @Override
  protected List<String> getViews() {
    return views;
  }

  @Override
  protected void createViews(Statement stmt) throws SQLException {
    String viewString = "CREATE MATERIALIZED VIEW %s AS select * FROM %s UNION ALL select * FROM %s;";
    String view1 = String.format(viewString, t, r, s);
    logger.info(view1);
    
    stmt.addBatch(view1);
    
    logger.info("view created");
    
  }

  @Override
  protected void dropTriggers() throws SQLException {
    String[] ops = {"INSERT", "UPDATE", "DELETE"};
    Statement stmt = conn.createStatement();
    for (String tablename : this.getTables()) {
      for (String op : ops) {
        String triggerFunc = tablename + "_" + op + "_func";
        String triggerName = getTriggerName(tablename, triggerFunc);
        String query = "DROP TRIGGER IF EXISTS " + triggerName + " on " + tablename + ";";
        logger.info(query);
        stmt.executeUpdate(query);
      }
    }
    
  }

  @Override
  protected void createTriggers(Statement stmt) throws SQLException {
    for (String table : this.getTables()){
    String insertFunc = this.setupTriggerFuncforView(table, "INSERT",
        genfuncBody("INSERT", table, views.get(0)));
    String deleteFunc = this.setupTriggerFuncforView(table, "DELETE",
        genfuncBody("DELETE", table, views.get(0)));
    String updateFunc = this.setupTriggerFuncforView(table, "UPDATE",
        genfuncBody("UPDATE", table, views.get(0)));
    attachTriggers(stmt, table, insertFunc, updateFunc, deleteFunc);
    }
    
  }

  private String genfuncBody(String op, String table, String view){
    StringBuilder sb = new StringBuilder();
    switch (op) {
      case "INSERT":
        sb.append("INSERT INTO ");
        sb.append(view);
        sb.append(" VALUES (");
        for (String col : this.fields) {
          sb.append("NEW.");
          sb.append(col);
          sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(");");
        break;
      case "UPDATE":
        sb.append("UPDATE ");
        sb.append(view);
        sb.append(" SET ");
        for (String col : this.fields) {
          sb.append(col);
          sb.append(" = NEW.");
          sb.append(col);
          sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(" WHERE ");
        for (String col : this.fields) {
          sb.append(col);
          sb.append(" = OLD.");
          sb.append(col);
          if (fields.get(fields.size() - 1) != col) { // col is NOT last
            sb.append(" AND ");
          }
        }
        sb.append(";");
        break;
      case "DELETE":
        sb.append("DELETE FROM ");
        sb.append(view);
        sb.append(" WHERE ");
        for (String col : fields) {
          sb.append(col);
          sb.append("=");
          sb.append("OLD.");
          sb.append(col);
          if (fields.get(fields.size() - 1) != col) { // col is  NOT last
            sb.append(" AND ");
          }
        }
        sb.append(";");
        break;
      default:
        return null;
        
    }
    return sb.toString();
  }


  @Override
  protected void createReverseTriggers(Statement stmt) throws SQLException {
    // TODO Auto-generated method stub
    for (String view : this.getViews()){ // Should be just one
      String insertFunc = this.setupTriggerFuncforView(view, "INSERT",
          genReverseFuncBody("INSERT", view, tables));
      String deleteFunc = this.setupTriggerFuncforView(view, "DELETE",
          genReverseFuncBody("DELETE", view, tables));
      String updateFunc = this.setupTriggerFuncforView(view, "UPDATE",
          genReverseFuncBody("UPDATE", view, tables));
      attachTriggers(stmt, view, insertFunc, updateFunc, deleteFunc);
      }
    
  }

  private String genReverseFuncBody(String operation, String view, List<String> tables) {
    StringBuilder sb = new StringBuilder();
    switch (operation) {
      case "INSERT":
        sb.append("INSERT INTO ");
        sb.append(tables.get(0));
        sb.append(" VALUES (");
        for (String col : this.fields) {
          sb.append("NEW.");
          sb.append(col);
          sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(");");

        break;
      case "UPDATE":
        for (String table: tables){
          sb.append("UPDATE ");
          sb.append(table);
          sb.append(" SET ");
          for (String col : this.fields) {
            sb.append(col);
            sb.append(" = NEW.");
            sb.append(col);
            sb.append(",");
          }
          sb.deleteCharAt(sb.length() - 1);
          sb.append(" WHERE ");
          for (String col : this.fields) {
            sb.append(col);
            sb.append(" = OLD.");
            sb.append(col);
            if (fields.get(fields.size() - 1) != col) { // col is NOT last
              sb.append(" AND ");
            }
          }
          sb.append(";");
        }
        break;
      case "DELETE":
        for (String table: tables){
          sb.append("DELETE FROM ");
          sb.append(table);
          sb.append(" WHERE ");
          for (String col : fields) {
            sb.append(col);
            sb.append("=");
            sb.append("OLD.");
            sb.append(col);
            if (fields.get(fields.size() - 1) != col) { // col is  NOT last
              sb.append(" AND ");
            }
          }
          sb.append(";");
        }
        break;
    }
    return null;
  }

  @Override
  public void rollbackSMO() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void commitSMO() {
    // TODO Auto-generated method stub
    
  }

}
