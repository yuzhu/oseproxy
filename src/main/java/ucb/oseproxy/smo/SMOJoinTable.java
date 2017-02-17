package ucb.oseproxy.smo;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

public class SMOJoinTable extends SMOAbstractCommand {
  private String r,s,t; 
  private String cond;
  private List<String> tables, views;
  private HashMap<String, List<String>> fieldsMap = null;
  
  private static Logger logger = Logger.getLogger(SMOPartitionTable.class.getName());
  
  public SMOJoinTable() {
    tables = new ArrayList<String>();
    views = new ArrayList<String>();
  }

  public SMOJoinTable(String r, String s, String t, String joincond) {
    this();
    tables.add(r);
    tables.add(s);
    views.add(t);
    this.r = r;
    this.s = s;
    this.t = t;
    this.cond = joincond;
  }
  
  public void connect(Connection conn){
    super.connect(conn);
    fieldsMap = new HashMap<String,  List<String>>();
    List<String> fields;
    // populate fields list
    fields = Utils.getColumns(conn, r);
    fieldsMap.put(r, fields);
    fields = Utils.getColumns(conn, s);
    fieldsMap.put(s, fields);
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
    String viewString = "CREATE MATERIALIZED VIEW %s AS select * FROM %s, %s where %s;";
    String view1 = String.format(viewString, t, r, s, this.cond);
    logger.info(view1);
    stmt.addBatch(view1);
    
    logger.info("views created");
  }

  @Override
  protected void createTriggers(Statement stmt) throws SQLException {
    for (String table : this.getTables()){
      String insertFunc = this.setupTriggerFuncforView(table, "INSERT",
          genfuncBody("INSERT", table, views.get(0), cond));
      String deleteFunc = this.setupTriggerFuncforView(table, "DELETE",
          genfuncBody("DELETE", table, views.get(0), cond));
      String updateFunc = this.setupTriggerFuncforView(table, "UPDATE",
          genfuncBody("UPDATE", table, views.get(0), cond));
      attachTriggers(stmt, table, insertFunc, updateFunc, deleteFunc);
      }

  }
  // select * from currtable, jointable into view where cond = true
  
  private String genfuncBody(String op, String table, String view, String cond) {
    String currtable = table;
    String jointable = "";
    List<String> currfields, joinfields;
    currfields = this.fieldsMap.get(table);
    
    StringBuffer sb = new StringBuffer();
    for (String tbl: this.getTables()){
      if (currtable!=tbl){
        jointable = tbl;
        joinfields = this.fieldsMap.get(jointable);
      }
    }
      
    switch (op) {
      case "INSERT":
        sb.append("INSERT INTO ");
        sb.append(view);
        
        sb.append(" WITH temptable AS ");
        sb.append(" (SELECT NEW.*)" );
        
        // With temtable as values(NEW.COL1, NEW.COL2, ...  )
        sb.append(" SELECT * FROM ");
        if (currtable.equals(this.r)) {
          sb.append("temptable ");
          sb.append(" , ");
          sb.append(s);
        } else {
          sb.append(r);
          sb.append(" , ");
          sb.append("temptable");
        }
        sb.append(" WHERE ");
        sb.append(cond.replaceAll(currtable, "temptable"));
        sb.append(" ;");
        break;
      case "DELETE":
          // delete from view where view.col = old.col
        sb.append("DELETE FROM ");
        sb.append(view);
        sb.append(" WHERE ");
        for (String col : currfields) {
          sb.append(col);
          sb.append("=");
          sb.append("OLD.");
          sb.append(col);
          if (currfields.get(currfields.size() - 1) != col) { // col is  NOT last
            sb.append(" AND ");
          }
        }
        sb.append(";");
        break;
      case "UPDATE":
        // delete from view where view.col = old.col
        sb.append("DELETE FROM ");
        sb.append(view);
        sb.append(" WHERE ");
        for (String col : currfields) {
          sb.append(col);
          sb.append("=");
          sb.append("OLD.");
          sb.append(col);
          if (currfields.get(currfields.size() - 1) != col) { // col is  NOT last
            sb.append(" AND ");
          }
        }
        sb.append(";");
        
        // Delete followed by an insert
        sb.append("INSERT INTO ");
        sb.append(view);
        
        sb.append(" WITH temptable AS ");
        sb.append(" (SELECT NEW.*)" );
        
        // With temtable as values(NEW.COL1, NEW.COL2, ...  )
        sb.append(" SELECT * FROM ");
        if (currtable.equals(this.r)) {
          sb.append("temptable ");
          sb.append(" , ");
          sb.append(s);
        } else {
          sb.append(r);
          sb.append(" , ");
          sb.append("temptable");
        }
        sb.append(" WHERE ");
        sb.append(cond.replaceAll(currtable, "temptable"));
        sb.append(" ;");
        break;
     
    }
    // TODO Auto-generated method stub
    return sb.toString();
  }

  @Override
  protected void createReverseTriggers(Statement stmt) throws SQLException {
    // TODO Auto-generated method stub

  }

}
