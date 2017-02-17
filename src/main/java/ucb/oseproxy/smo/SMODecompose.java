package ucb.oseproxy.smo;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class SMODecompose extends SMOAbstractCommand {
  private String table, tablecolumns, view1columns, view2columns;
  
  private static Logger logger = Logger.getLogger(SMODecompose.class.getName());
  public SMODecompose(){
    this.cmd = Command.DECOMPOSE_TABLE;
  }
  public SMODecompose(List<String> args) {
    if (args.size() != 4 && args.size() != 6) {
      logger.warning("Number of options not valid for DECOMPOSE_TABLE");
    }
    table = args.get(0);
    tablecolumns = args.get(1);
    view1columns = args.get(2);
    view2columns = args.get(3);
    tables.add(table);
    if (args.size() == 4) {
      views.add(genViewName(table, 1));
      views.add(genViewName(table, 2));
    } else {
      views.add(args.get(4));
      views.add(args.get(5));
    }
  }
  
  public SMODecompose(String tablename, String view1, String view2, String collist1, String collist2) {
    table = tablename;
    tables.add(table);
    view1columns = collist1;
    view2columns = collist2;
    views.add(view1);
    views.add(view2);
  }
  public void connect(Connection conn){
    super.connect(conn);
    if (tablecolumns == null)
      Utils.getColumns(conn, table);
  }
  
  public SMODecompose(Connection conn, Command cmd2, List<String> args) {
    this(args);
    this.connect(conn);
  }

  private String genViewName(String tablename, int index) {
    return "OSE_VIEW" + index + "_" + tablename;
  }
  
  private String getViewName(int index) {
    return views.get(index-1);
  }

  protected void createViews(Statement stmt) throws SQLException {
    
    
    String viewString = "CREATE MATERIALIZED VIEW %s AS select %s, %s FROM %s;";
    String view1 = String.format(viewString, getViewName(1), tablecolumns, view1columns, table);
    logger.info(view1);
    String view2 = String.format(viewString, getViewName(2), tablecolumns, view2columns, table);
    logger.info(view2);
    stmt.addBatch(view1);
    stmt.addBatch(view2);
    
    logger.info("view created");
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
          sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
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
          if (cols.get(cols.size() - 1) != col) { // col is last
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
          if (cols.get(cols.size() - 1) != col) { // col is last
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
          if (cols.get(cols.size() - 1) != col) { // col is last
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

  private String genReverseFuncBody(String operation, String table, String view, String collistKey,
      String collistView, String collistOther) {
    // key|view => key|view|other
    List<String> keysCols = Arrays.asList(collistKey.split(","));
    List<String> viewCols = Arrays.asList(collistView.split(","));

    List<String> keysview = new ArrayList<String>();
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
        // Propagate data from the insertion into view to the original table
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
        sb.deleteCharAt(sb.length() - 1);
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
        sb.deleteCharAt(sb.length() - 1);
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
        sb.deleteCharAt(sb.length() - 1);
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
  protected void createTriggers(Statement stmt) throws SQLException {
    String insertFunc = this.setupTriggerFuncforView(table, "INSERT",
        genfuncBody("INSERT", table, getViewName(1), tablecolumns + "," + view1columns)
            + genfuncBody("INSERT", table, getViewName(2), tablecolumns + "," + view2columns));
    String deleteFunc = this.setupTriggerFuncforView(table, "DELETE",
        genfuncBody("DELETE", table, getViewName(1), tablecolumns + "," + view1columns)
            + genfuncBody("DELETE", table, getViewName(2), tablecolumns + "," + view2columns));
    String updateFunc = this.setupTriggerFuncforView(table, "UPDATE",
        genfuncBody("UPDATE", table, getViewName(1), tablecolumns + "," + view1columns)
            + genfuncBody("UPDATE", table, getViewName(2), tablecolumns + "," + view2columns));
    attachTriggers(stmt, table, insertFunc, updateFunc, deleteFunc);
  }

  @Override
  protected void createReverseTriggers(Statement stmt) throws SQLException {
    
    String insertView1Func = this.setupTriggerFuncforView(getViewName(1), "INSERT",
        genReverseFuncBody("INSERT", table, getViewName(1), tablecolumns, view1columns, view2columns));
    String deleteView1Func = this.setupTriggerFuncforView(getViewName(1), "DELETE",
        genReverseFuncBody("DELETE", table, getViewName(1), tablecolumns, view1columns, view2columns));
    String updateView1Func = this.setupTriggerFuncforView(getViewName(1), "UPDATE",
        genReverseFuncBody("UPDATE", table, getViewName(1), tablecolumns, view1columns, view2columns));

    attachTriggers(stmt, getViewName(1), insertView1Func, updateView1Func, deleteView1Func);
    
    
    String insertView2Func = this.setupTriggerFuncforView(getViewName(2), "INSERT",
        genReverseFuncBody("INSERT", table, getViewName(2), tablecolumns, view2columns, view1columns));
    String deleteView2Func = this.setupTriggerFuncforView(getViewName(2), "DELETE",
        genReverseFuncBody("DELETE", table, getViewName(2), tablecolumns, view2columns, view1columns));
    String updateView2Func = this.setupTriggerFuncforView(getViewName(2), "UPDATE",
        genReverseFuncBody("UPDATE", table, getViewName(2), tablecolumns, view2columns, view1columns));

    attachTriggers(stmt, getViewName(2), insertView2Func, updateView2Func, deleteView2Func);
    
 
  }
  @Override
  public boolean commitSMO() {
    
    return true;
  }
  @Override
  public boolean rollbackSMO() {
    Statement stmt;
    try {
      stmt = conn.createStatement();
      dropViews(stmt);
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
