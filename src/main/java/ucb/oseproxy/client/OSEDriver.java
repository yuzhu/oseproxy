package ucb.oseproxy.client;

import java.sql.Connection;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Logger;

/* Example of a connection URL for OSEDriver would be jdbc:ose:postgresql://proxyhost:proxyport */

public class OSEDriver implements Driver {
  private static Driver INSTANCE = new OSEDriver();

  static {
    try {
      DriverManager.registerDriver(INSTANCE);
    } catch (SQLException e) {
      throw new IllegalStateException("Could not register OSEDriver with DriverManager", e);
    }
  }

  public Connection connect(String url, Properties info) throws SQLException {
    if (url == null) {
      throw new SQLException("url is required");
    }

    if (!acceptsURL(url)) {
      return null;
    }

    // now we have the url validated
    return connectToProxy(url, info);
  }

  private OSEConnection connectToProxy(String url, Properties info) {

    String[] parsedURL = url.split("//")[1].split(":");
    String proxyHost = parsedURL[0];
    String proxyPort = parsedURL[1];
    // connect netty/protobuf stuff..
    // TODO: wrap netty connection inside OSEConnection.
    return new OSEConnection();
  }

  public boolean acceptsURL(String url) throws SQLException {
    if (url != null && url.startsWith("jdbc:ose:")) {
      return true;
    } else {
      return false;
    }
  }

  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    DriverPropertyInfo[] dpi = new DriverPropertyInfo[info.size()];
    for (DriverPropertyInfo elem : dpi) {
      // elem.
    }
    return dpi;
  }

  public int getMajorVersion() {
    return 0;
  }

  public int getMinorVersion() {
    // TODO Auto-generated method stub
    return 1;
  }

  public boolean jdbcCompliant() {
    // TODO Auto-generated method stub
    return false;
  }

  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Feature not supported");
  }

}
