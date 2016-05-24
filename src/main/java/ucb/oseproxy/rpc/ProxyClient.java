package ucb.oseproxy.rpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import ucb.oseproxy.util.ProxyUtil;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

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
  public String connect(String dbhost, int port, String username, String password) {
    logger.info("Will try to connect to DB at  " + dbhost + ":" + port + "u:" + username);

    ConnRequest request = ConnRequest.newBuilder().setClientID(uuid).setHost(dbhost).setPort(port)
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

  public static void main(String[] args) throws IOException, InterruptedException {
    String dbURL = "";
    int dbPort = 5432;
    String username = "";
    String password = "";
    if (args.length != 4) {
      logger.info("Using some default values for parameters");
    } else {
      dbURL = args[0];
      dbPort = Integer.parseInt(args[1]);
      username = args[2];
      password = args[3];
    }
    
    ProxyClient client = new ProxyClient("localhost", 50051);
    try {
      String connId = client.connect(dbURL, dbPort, username, password);
      logger.info("Connection id " + connId);
    } finally {
      client.shutdown();
    }
  }

}
