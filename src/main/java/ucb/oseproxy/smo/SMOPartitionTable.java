package ucb.oseproxy.smo;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class SMOPartitionTable extends SMOAbstractCommand {
  private String r,s,t; 
  private String cond;
  private List<String> tables, views;
  private List<String> fields = null;
  
  private static Logger logger = Logger.getLogger(SMOPartitionTable.class.getName());
  
  public SMOPartitionTable() {
     tables = new ArrayList<String>();
     views = new ArrayList<String>();
  }
  
  public SMOPartitionTable(String r, String s, String t, String condfors) {
    this();
    tables.add(r);
    views.add(s);
    views.add(t);
    this.cond = condfors;
  }
  
  public void connect(Connection conn){
    super.connect(conn);
    // populate fields list
    if (fields == null){
      fields = Utils.getColumns(conn, r);
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
  

  public String getCond() {
    return cond;
  }

  public void setCond(String cond) {
    this.cond = cond;
  }

  @Override
  protected void createViews(Statement stmt) throws SQLException {
    String viewString = "CREATE MATERIALIZED VIEW %s AS select * FROM %s where %s;";
    String view2String = "CREATE MATERIALIZED VIEW %s AS select * FROM %s where NOT (%s);";
    String view1 = String.format(viewString, s, r, this.cond);
    String view2 = String.format(viewString, t, r, this.cond);
    logger.info(view1);
    logger.info(view2);
    stmt.addBatch(view1);
    stmt.addBatch(view2);
    
    logger.info("views created");

  }

  @Override
  protected void createTriggers(Statement stmt) throws SQLException {
    for (String table : this.getTables()){
      String insertFunc = this.setupTriggerFuncforView(table, "INSERT",
          genfuncBody("INSERT", table, views.get(0), views.get(1), cond));
      String deleteFunc = this.setupTriggerFuncforView(table, "DELETE",
          genfuncBody("DELETE", table, views.get(0), views.get(1), cond));
      String updateFunc = this.setupTriggerFuncforView(table, "UPDATE",
          genfuncBody("UPDATE", table, views.get(0), views.get(1), cond));
      attachTriggers(stmt, table, insertFunc, updateFunc, deleteFunc);
    }
  }
  // if insert, depending on condition, insert into a view
  //
  
  private String genfuncBody(String op, String table, String view, String view2, String cond) {
    StringBuilder sb = new StringBuilder();
    String newcond = cond.replaceAll(table, "NEW");
    String oldcond = cond.replaceAll(table, "OLD");
    
    switch (op) {
      case "INSERT":
        sb.append ("IF (");
        sb.append (newcond);
        sb.append( ") THEN ");
        //insert into view
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
        
        //else insert into view2
        sb.append(" ELSE ");
        
        sb.append("INSERT INTO ");
        sb.append(view2);
        sb.append(" VALUES (");
        for (String col : this.fields) {
          sb.append("NEW.");
          sb.append(col);
          sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(");");
        sb.append(" END IF;");
        break;
      case "UPDATE":
        //Remove the old
        sb.append("IF (");
        sb.append(oldcond);
        sb.append(") THEN ");
        // delete from view where view.col = old.col
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
        
        sb.append(" ELSE ");
        
        // delete from view2
        sb.append("DELETE FROM ");
        sb.append(view2);
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
        
        sb.append(" END IF; ");
        // Insert the new
        sb.append("IF (");
        sb.append(newcond);
        sb.append(") THEN ");
        // Insert into view because cond is met
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
        
        
        sb.append(" ELSE ");
        
     // Insert into view2 because cond is NOT met for the new entry
        sb.append("INSERT INTO ");
        sb.append(view2);
        sb.append(" VALUES (");
        for (String col : this.fields) {
          sb.append("NEW.");
          sb.append(col);
          sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(");");
        
        
        sb.append(" END IF; ");
        
        
        break;
      case "DELETE":
        sb.append("IF (");
        sb.append(oldcond);
        sb.append(") THEN ");
        // delete from view where view.col = old.col
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
        
        sb.append(" ELSE ");
        
        // delete from view2
        sb.append("DELETE FROM ");
        sb.append(view2);
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
        
        sb.append(" END IF; ");
        break;
      default:
        return null;
        
    }
    return sb.toString();
  }

  @Override
  protected void createReverseTriggers(Statement stmt) throws SQLException {
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

  private String genReverseFuncBody(String op, String view, List<String> tables) {
    StringBuilder sb = new StringBuilder();
    String table = tables.get(0);
    String newcond = cond.replaceAll(table, "NEW");
    String oldcond = cond.replaceAll(table, "OLD");
    
    switch (op) {
      case "INSERT":
        sb.append("INSERT INTO ");
        sb.append(table);
        sb.append(" VALUES (");
        for (String col : this.fields) {
          sb.append("NEW.");
          sb.append(col);
          sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(");");
        
        break;
      case "DELETE":
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
        break;
      case "UPDATE":
          // potentially broken unless we assume unique entries in the table
          sb.append("UPDATE ");
          sb.append(table);
          sb.append(" SET ");
          for (String col : this.fields) {
            sb.append(col);
            sb.append(" = NEW.");
            sb.append(col);
            sb.append(",");
        
          sb.deleteCharAt(sb.length() - 1);
          sb.append(" WHERE ");
          for (String col2 : this.fields) {
            sb.append(col2);
            sb.append(" = OLD.");
            sb.append(col2);
            if (fields.get(fields.size() - 1) != col) { // col is NOT last
              sb.append(" AND ");
            }
          }
          sb.append(";");
        }
        break;
      default: 
        return null;
    }
    return sb.toString();
  }

}
