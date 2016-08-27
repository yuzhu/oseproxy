package ucb.oseproxy.smo;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class SMODecompose extends SMOAbstractCommand {
  private String table, collista, collistb, collistc;
  private List<String> views = new ArrayList<String> ();
  private List<String> tables = new ArrayList<String> ();
  private static Logger logger = Logger.getLogger(SMOAbstractCommand.class.getName());
  
  public SMODecompose(Connection conn, Command cmd2, List<String> options) {
    super(conn, cmd2, options);
    if (args.size() != 4) {
      logger.warning("Number of options not valid for DECOMPOSE_TABLE");
    }
    table = args.get(0);
    collista = args.get(1);
    collistb = args.get(2);
    collistc = args.get(3);
    tables.add(table);
    views.add(getViewName(table, 1));
    views.add(getViewName(table, 2));
  }
  
  private String getViewName(String tablename, int index) {
    return "OSE_VIEW" + index + "_" + tablename;
  }
  
  protected List<String> getViews() {
    return views;
  }
  
  protected void createViews() throws SQLException {
    Statement stmt = conn.createStatement();
    dropViews();
    String viewString = "CREATE MATERIALIZED VIEW %s AS select %s, %s FROM %s;" ;
    String view1 = String.format(viewString, getViewName(table,1), collista, collistb , table);
    logger.info(view1);
    String view2 = String.format(viewString, getViewName(table,2), collista, collistc , table);
    logger.info(view2);
    stmt.addBatch(view1);
    stmt.addBatch(view2);
    stmt.executeBatch();
  }

  
  private String genfuncBody(String operation, String table, String view, String collist) {
    List<String> cols = Arrays.asList(collist.split(","));
    StringBuilder sb = new StringBuilder();
    switch (operation) {
      case "INSERT":
         sb.append("INSERT INTO ");
         sb.append(view);
         sb.append(" VALUES (");
         for (String col : cols) {
           sb.append("NEW.");
           sb.append(col);
           if (cols.get(cols.size()-1) != col) { // col is last
             sb.append(",");
           }
         }
         sb.append(");");
        break;
      case "DELETE":
        sb.append("DELETE FROM ");
        sb.append(view);
        sb.append(" WHERE ");
        for (String col : cols) {
          
          sb.append(col);
          sb.append("=");
          sb.append("OLD.");
          sb.append(col);
          if (cols.get(cols.size()-1) != col) { // col is last
            sb.append(" AND ");
          }
        }
        sb.append(";");
       break;
      case "UPDATE":
        sb.append("DELETE FROM ");
        sb.append(view);
        sb.append(" WHERE ");
        for (String col : cols) {
          
          sb.append(col);
          sb.append("=");
          sb.append("OLD.");
          sb.append(col);
          if (cols.get(cols.size()-1) != col) { // col is last
            sb.append(" AND ");
          }
        }
        sb.append(";");
        
        sb.append("INSERT INTO ");
        sb.append(view);
        sb.append(" VALUES (");
        for (String col : cols) {
          sb.append("NEW.");
          sb.append(col);
          if (cols.get(cols.size()-1) != col) { // col is last
            sb.append(",");
          }
        }
        sb.append(");");
        
        
        break;
     default:
       logger.warning("Operation not supported:  " + operation);
       return null;
    }
    return sb.toString();
  }
  
  private String genReverseFuncBody(String operation, String table, String view, 
      String collistKey, String collistView, String collistOther) {
    // key|view => key|view|other
    List<String> keysCols = Arrays.asList(collistKey.split(","));
    List<String> viewCols = Arrays.asList(collistView.split(","));
    
    List<String> keysview = new ArrayList<String> ();
    keysview.addAll(keysCols);
    keysview.addAll(viewCols);
    List<String> otherCols = Arrays.asList(collistOther.split(","));
    StringBuilder sb = new StringBuilder();
    switch (operation) {
      // TODO: consider refactoring into the following.
      // sb.append(genInsert("NEW", collist, defaultlist, destname))
      case "INSERT":
         sb.append("INSERT INTO ");
         sb.append(table);
         sb.append(" VALUES (");
         // Propagate data from the  insertion into view to the original table 
         for (String col : keysCols) {
           sb.append("NEW.");
           sb.append(col);
           sb.append(",");
           
         }
         for (String col : viewCols) {
           sb.append("NEW.");
           sb.append(col);
           sb.append(",");
         }
         
         // Insert null value or default values for things that are not in the insertion.
         for (String col : otherCols) {
           sb.append("DEFAULT");
           sb.append(",");
         }
         // delete the last character
         sb.deleteCharAt(sb.length()-1);
         sb.append(");");
         break;
      case "UPDATE":
        sb.append("UPDATE ");
        sb.append(table);
        sb.append(" SET ");
        for (String col : viewCols) {
          sb.append(col);
          sb.append("=");
          sb.append("NEW.");
          sb.append(col);
          sb.append(",");
        }
        sb.deleteCharAt(sb.length()-1);
        sb.append(" WHERE ");
        for (String col : keysCols) {
          sb.append(col);
          sb.append("=");
          sb.append("OLD.");
          sb.append(col);
          sb.append(" AND ");
        }
        for (String col : viewCols) {
          sb.append(col);
          sb.append("=");
          sb.append("OLD.");
          sb.append(col);
          sb.append(" AND ");
        }
        sb.append(" TRUE;");
        
      case "DELETE":
        // try to Update original table with null/default value, if it fails, then delete the rows.
        sb.append("UPDATE ");
        sb.append(table);
        sb.append(" SET ");
        for (String col : viewCols) {
          sb.append(col);
          sb.append("=");
          sb.append("DEFAULT ,");
        }
        sb.deleteCharAt(sb.length()-1);
        sb.append(" WHERE ");
        for (String col : keysCols) {
          sb.append(col);
          sb.append("=");
          sb.append("OLD.");
          sb.append(col);
          sb.append(" AND ");
        }
        for (String col : viewCols) {
          sb.append(col);
          sb.append("=");
          sb.append("OLD.");
          sb.append(col);
          sb.append(" AND ");
        }
        sb.append(" TRUE;");
        break;
    }
    
    return sb.toString();   
  }
  @Override
  protected void createTriggers() throws SQLException {
    
    String insertFunc = this.setupTriggerFuncforView(table, "INSERT", genfuncBody("INSERT", table, getViewName(table,1), collista + "," + collistb) 
        + genfuncBody("INSERT", table, getViewName(table,2), collista + "," + collistc));
    String deleteFunc = this.setupTriggerFuncforView(table, "DELETE", genfuncBody("DELETE", table, getViewName(table,1), collista + "," + collistb) 
        + genfuncBody("DELETE", table, getViewName(table,2), collista + "," + collistc));
    String updateFunc = this.setupTriggerFuncforView(table, "UPDATE", genfuncBody("UPDATE", table, getViewName(table,1), collista + "," + collistb) 
        + genfuncBody("UPDATE", table, getViewName(table,2), collista + "," + collistc));
    Statement stmt = conn.createStatement();
    attachTriggers(stmt, table, insertFunc, updateFunc, deleteFunc);
  }
  
  @Override
  protected void createReverseTriggers() throws SQLException {
    String insertView2Func = this.setupTriggerFuncforView(getViewName(table, 2), "INSERT",
        genReverseFuncBody("INSERT", table, getViewName(table, 2), collista, collistc, collistb));
    String deleteView2Func = this.setupTriggerFuncforView(getViewName(table, 2), "DELETE",
        genReverseFuncBody("DELETE", table, getViewName(table, 2), collista, collistc, collistb));
    String updateView2Func = this.setupTriggerFuncforView(getViewName(table, 2), "UPDATE",
        genReverseFuncBody("UPDATE", table, getViewName(table, 2), collista, collistc, collistb));

    Statement stmt = conn.createStatement();
    attachTriggers(stmt, getViewName(table, 2), insertView2Func, updateView2Func, deleteView2Func);
  }

  @Override
  public void rollbackSMO() {
    // TODO Auto-generated method stub
    
  }

}
