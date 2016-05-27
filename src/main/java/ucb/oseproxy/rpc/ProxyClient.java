package ucb.oseproxy.rpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import ucb.oseproxy.client.OSEResultSet;
import ucb.oseproxy.util.ProtobufEnvelope;
import ucb.oseproxy.util.ProxyUtil;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;

public class ProxyClient {
  private static final Logger logger = Logger.getLogger(ProxyClient.class.getName());

  private final ManagedChannel channel;
  private final OSEProxyGrpc.OSEProxyBlockingStub blockingStub;
  private final String uuid;

  public ProxyClient(String host, int port) {
    channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).build();
    blockingStub = OSEProxyGrpc.newBlockingStub(channel);
    uuid = ProxyUtil.randomId();
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  /** Say hello to server. */
  public String connect(String dbhost, int port, String dbname, String username, String password) {
    logger.info("Will try to connect to DB at  " + dbhost + ":" + port + "u:" + username);

    ConnRequest request = ConnRequest.newBuilder().setClientID(uuid).setHost(dbhost).setPort(port).setDbname(dbname)
        .setUsername(username).setPassword(password).build();
    ConnReply response;
    try {
      response = blockingStub.getConn(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return null;
    }
    return response.getConnId();
  }
  
  public ResultSet execQuery(String connId, String query) {
    logger.info("Running query on connection " + connId + " query : " + query);
    QueryRequest request = QueryRequest.newBuilder().setConnId(connId).setQuery(query).build();
    QueryReply response;
    try {
      response = blockingStub.execQuery(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return null;
    }
    OSEResultSet rs = new OSEResultSet(blockingStub, response.getResultSetId(), response.getSchema());

    return rs;
  }
  
  public static void main(String[] args) throws IOException, InterruptedException {
    String dbURL = "";
    int dbPort = 5432;
    String dbname = "";
    String username = "";
    String password = "";
    if (args.length != 5) {
      logger.info("Using some default values for parameters");
    } else {
      dbURL = args[0];
      dbPort = Integer.parseInt(args[1]);
      dbname = args[2];
      username = args[3];
      password = args[4];
    }
    
    ProxyClient client = new ProxyClient("localhost", 50051);
    try {
      // client.readRow();
      // client.readRow();
      String connId = client.connect(dbURL, dbPort, dbname, username, password);
      logger.info("Connection id " + connId);
      ResultSet rs = client.execQuery(connId, "select * from persons");
      while (rs.next()) {
      }
    } catch (SQLException e){
    
    }
    finally {
   
      client.shutdown();
    }
  }

}
