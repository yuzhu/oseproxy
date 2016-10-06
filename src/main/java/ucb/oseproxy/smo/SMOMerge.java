package ucb.oseproxy.smo;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;


// MERGE TABLE (R,S) INTO T, assuming R, S has the same structure
public class SMOMerge extends SMOAbstractCommand {

  private static Logger logger = Logger.getLogger(SMOAbstractCommand.class.getName());
  private String r,s,t;
  private List<String> tables, views;
  private List<String> fields = null;
  
  
  public void init(String r, String s, String t) {
    this.r=r;
    this.s=s;
    this.t=t;
    tables = new ArrayList<String>();
    views = new ArrayList<String>();
    tables.add(r);
    tables.add(s);
    views.add(t);
    

    
  }
  
  public void connect(Connection conn){
    super.connect(conn);
    // populate fields list
    if (fields == null){
      fields = Utils.getColumns(conn, r);
    }
  }

  public SMOMerge(String r, String s, String t) {
    this.cmd = Command.MERGE_TABLE;
    init(r,s,t);
  }
  
  public SMOMerge(List<String> options) {
    this.cmd = Command.MERGE_TABLE;
    if (options.size() != 3) {
      logger.warning("Number of options not valid for DECOMPOSE_TABLE");
    }
    init(options.get(0), options.get(1),options.get(2));
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
      default:
        return null;
    }
    return sb.toString();
  }
}
