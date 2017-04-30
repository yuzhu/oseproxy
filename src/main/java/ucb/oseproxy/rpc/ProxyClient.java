package ucb.oseproxy.rpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import ucb.oseproxy.client.OSEResultSet;
import ucb.oseproxy.measure.MonitorTask;
import ucb.oseproxy.smo.SMOCommand.Command;
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

    ConnRequest request = ConnRequest.newBuilder().setClientID(uuid).setHost(dbhost).setPort(port)
        .setDbname(dbname).setUsername(username).setPassword(password).build();
    ConnReply response;
    try {
      response = blockingStub.getConn(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return null;
    }
    return response.getConnId();
  }
  
  public int execUpdate(String connId, String query) {
    // logger.info("Running update on connection " + connId + " query : " + query);
    UpdateRequest request = UpdateRequest.newBuilder().setConnId(connId).setQuery(query).build();
    UpdateReply response;
    try {
      response = blockingStub.execUpdate(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return 0;
    }
    return response.getRowCount();

  }
  

  public ResultSet execQuery(String connId, String query) {
    // logger.info("Running query on connection " + connId + " query : " + query);
    QueryRequest request = QueryRequest.newBuilder().setConnId(connId).setQuery(query).build();
    QueryReply response;
    try {
      response = blockingStub.execQuery(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return null;
    }
    OSEResultSet rs =
        new OSEResultSet(blockingStub, response.getResultSetId(), response.getSchema());

    return rs;
  }
  
  public String issueSMO(String connId, Command cmd, String[] opts) {
    logger.info("Running SMO cmd connection " + connId + " cmd : " + cmd);
    SMORequest.Builder requestBuilder = SMORequest.newBuilder().setConnId(connId).setCmd(cmd.ordinal());
    for (String opt :opts) {
      requestBuilder.addArg(opt);
    }
    SMORequest request = requestBuilder.build();
    SMOReply response;
    try {
      response = blockingStub.execSMO(request);
      return response.getSmoId();
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return null;
    }
  }
  
  public String issueSMOString(String connId, String smoCmd) {
    logger.info("Running SMO cmd connection " + connId + " cmd : " + smoCmd);
    
    SMOStringRequest.Builder requestBuilder = SMOStringRequest.newBuilder().setConnId(connId).setSmoCmd(smoCmd);
    // requestBuilder.
    SMOStringRequest request = requestBuilder.build();
    SMOReply response;
    try {
      response = blockingStub.execSMOString(request);
      return response.getSmoId();
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return null;
    }
  }
  
  

  public static void main(String[] args) throws IOException, InterruptedException {
    String dbURL = "";
    int dbPort = 5432;
    String dbname = "";
    String username = "";
    String password = "";
    String inputfile = "commands";
    if (args.length != 5) {
      logger.info("Using some default values for parameters");
    } else {
      dbURL = args[0];
      dbPort = Integer.parseInt(args[1]);
      dbname = args[2];
      username = args[3];
      password = args[4];
    }
    
    
    // BackgroundThread background  = new BackgroundThread(dbURL, dbPort, dbname, username, password);
    
    
    ClientThread schanger = new SMOThread(dbURL, dbPort, dbname, username, password);
    
    ProxyClient client = new ProxyClient("localhost", 50051);
    // background.start();
    Thread.sleep(10000);
    schanger.start();
    //System.in.read();
    schanger.join();
    // background.join(5000);
    logger.info("All threads finished");
  }

  public void commitSMO(String connId, String smoId) {
    // TODO Auto-generated method stub
    return;
    
  }



}

