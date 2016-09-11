package ucb.oseproxy.smo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Utils {

  public static List<String> getColumns(Connection conn, String r) {
    Statement stmt;
    // populate fields list
    List<String> fields = new ArrayList<String>();
    try {
      stmt = conn.createStatement();
      // Verify R and S has same columns so they can merge
      ResultSet rsr = stmt.executeQuery("select * from " + r + " where 0=1");
      ResultSetMetaData rsmdr = rsr.getMetaData();
      for (int i = 1; i<= rsmdr.getColumnCount(); i++){
        fields.add(rsmdr.getColumnName(i));
      }
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return fields;
  }

}
