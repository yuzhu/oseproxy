package ucb.oseproxy.rpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ProxyClient {
  private static final Logger logger = Logger.getLogger(ProxyClient.class.getName());

  private final ManagedChannel channel;
  private final OSEProxyGrpc.OSEProxyBlockingStub blockingStub;

  public ProxyClient(String host, int port) {
    channel = ManagedChannelBuilder.forAddress(host, port)
        .usePlaintext(true)
        .build();
    blockingStub = OSEProxyGrpc.newBlockingStub(channel);
  }
  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }
  
  /** Say hello to server. */
  public void connect(String dbhost, int port, String username, String password) {
    logger.info("Will try to greet " + dbhost +":" + port + "u:" + username );
    
    ConnRequest request = ConnRequest.newBuilder().setHost(dbhost)
                                    .setPort(port).setUsername(username).setPassword(password).build();
    ConnReply response;
    try {
      response = blockingStub.getConn(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }
    logger.info("Status: " + response.getStatus());
  }

}
