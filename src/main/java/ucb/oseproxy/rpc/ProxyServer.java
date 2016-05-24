package ucb.oseproxy.rpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.stub.StreamObserver;
import ucb.oseproxy.server.OSEServer;


import java.io.IOException;
import java.util.logging.Logger;


public class ProxyServer {

  private static final Logger logger = Logger.getLogger(ProxyServer.class.getName());

  /* The port on which the server should run */
  private int port = 50051;
  private Server server;

  private void start() throws IOException {
    server = ServerBuilder.forPort(port)
        .addService(new OSEProxyImpl())
        .build()
        .start();
    logger.info("Server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        ProxyServer.this.stop();
        System.err.println("*** server shut down");
      }
    });
  }

  private void stop() {
    if (server != null) {
      server.shutdown();
    }
  }
  
  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  
  /**
   * Main launches the server from the command line.
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    final ProxyServer server = new ProxyServer();
    server.start();
    server.blockUntilShutdown();
  }

  private class OSEProxyImpl extends OSEProxyGrpc.AbstractOSEProxy {

    @Override
    public void getConn(ConnRequest req, StreamObserver<ConnReply> responseObserver) {
      logger.info("Client connection request " + req.getHost());;
      String code = OSEServer.getInstance().connectClient(req.getClientID(), req.getHost(), req.getPort(), req.getUsername(), req.getPassword());
      ConnReply reply;  
      if (code == null) {
        reply = ConnReply.newBuilder().setConnId(code).setStatus("Disconnected").build();
      } else {
        reply = ConnReply.newBuilder().setConnId(code).setStatus("Connected").build();
      }
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
  }
  
}
