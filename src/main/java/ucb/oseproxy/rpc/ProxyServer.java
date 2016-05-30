package ucb.oseproxy.rpc;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import javax.management.Descriptor;

import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import ucb.oseproxy.server.OSEServer;
import ucb.oseproxy.smo.SMOCommand.Command;
import ucb.oseproxy.util.DynamicSchema;
import ucb.oseproxy.util.ProtobufEnvelope;


public class ProxyServer {

  private static final Logger logger = Logger.getLogger(ProxyServer.class.getName());

  /* The port on which the server should run */
  private int port = 50051;
  private Server server;

  private void start() throws IOException {
    server = ServerBuilder.forPort(port).addService(new OSEProxyImpl()).build().start();
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
    Map<String, DynamicSchema> rsProtoMap;

    public OSEProxyImpl() {
      super();
      rsProtoMap = new HashMap<String, DynamicSchema>();
    }

    @Override
    public void getConn(ConnRequest req, StreamObserver<ConnReply> responseObserver) {
      logger.info("Client connection request " + req.getHost());
      String code = OSEServer.getInstance().connectClient(req.getClientID(), req.getHost(),
          req.getPort(), req.getDbname(), req.getUsername(), req.getPassword());
      ConnReply reply;
      if (code == null) {
        reply = ConnReply.newBuilder().setConnId(code).setStatus("Disconnected").build();
      } else {
        reply = ConnReply.newBuilder().setConnId(code).setStatus("Connected").build();
      }
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }

    private DynamicSchema extractSchema(String resultSetId, ResultSetMetaData rsmd)
        throws java.sql.SQLException {
      DynamicSchema ds = new DynamicSchema();
      for (int i = 1; i <= rsmd.getColumnCount(); i++) {
        int tp = rsmd.getColumnType(i);
        switch (tp) {
          case java.sql.Types.INTEGER:
            ds.addField(rsmd.getColumnName(i),
                DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32);
            break;
          case java.sql.Types.VARCHAR:
            ds.addField(rsmd.getColumnName(i),
                DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
            break;
          case java.sql.Types.BOOLEAN:
            ds.addField(rsmd.getColumnName(i),
                DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL);
            break;
          case java.sql.Types.DOUBLE:
            ds.addField(rsmd.getColumnName(i),
                DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE);
            break;
          default:
            logger.warning("Data type not currently supported");
            break;
        }
      }
      ds.setName(resultSetId);
      this.rsProtoMap.put(resultSetId, ds);
      return ds;
    }

    private DynamicSchema getSchema(String resultSetId) {
      return this.rsProtoMap.get(resultSetId);
    }

    @Override
    public void execQuery(QueryRequest req, StreamObserver<QueryReply> responseObserver) {
      logger.info("Executing query on behalf of " + req.getConnId() + "  " + req.getQuery());
      String resultset = OSEServer.getInstance().execQuery(req.getConnId(), req.getQuery());
      ResultSet rs = OSEServer.getInstance().getResultSet(resultset);
      QueryReply reply = null;
      if (rs == null) {
        reply = QueryReply.newBuilder().setResultSetId(null).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
        return;
      }

      DynamicSchema ds = null;
      try {
        ResultSetMetaData rsmd = rs.getMetaData();
        ds = this.extractSchema(resultset, rsmd);
      } catch (java.sql.SQLException e) {
        logger.warning("Failed to extract schema for resultset " + resultset);
      }
      if (ds != null) {
        reply =
            QueryReply.newBuilder().setSchema(ds.getDescProto()).setResultSetId(resultset).build();
      } else {
        reply = QueryReply.newBuilder().setResultSetId(null).build();
      }
      responseObserver.onNext(reply);
      responseObserver.onCompleted();

    }
    
    @Override
    public void execUpdate(UpdateRequest req, StreamObserver<UpdateReply> responseObserver) {
      // logger.info("Executing update on behalf of " + req.getConnId() + "  " + req.getQuery());
      int returnVal = OSEServer.getInstance().execUpdate(req.getConnId(), req.getQuery());
      UpdateReply reply = null;
      reply = UpdateReply.newBuilder().setRowCount(returnVal).build();
      
      responseObserver.onNext(reply);
      responseObserver.onCompleted();

    }
    
    // In QueryReply, we include the metadata for the resultset as well

    @Override
    public void readRow(RowRequest req, StreamObserver<RowReply> responseObserver) {
      logger.info("read row called");
      Map<String, Object> row = OSEServer.getInstance().getNextRow(req.getResultSetId());
      RowReply reply = null;
      if (row == null) {
        reply = RowReply.newBuilder().setValid(false).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
        return;
      }

      DynamicSchema ds = this.getSchema(req.getResultSetId());
      Descriptors.Descriptor desc = ds.getDescriptor();

      DynamicMessage.Builder dmBuilder = DynamicMessage.newBuilder(desc);
      for (String name : row.keySet()) {
        dmBuilder.setField(desc.findFieldByName(name), row.get(name));
      }

      DynamicMessage dm = dmBuilder.build();
      Any any = Any.pack(dm);

      reply = RowReply.newBuilder().setValid(true).setRow(any).build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();

    }
    
    @Override
    public void execSMO(SMORequest req, StreamObserver<SMOReply> responseObserver) {
      logger.info("Executing query on behalf of " + req.getConnId() + "cmd: " + req.getCmd());
      SMOReply reply = null;
      OSEServer.getInstance().execSMO(req.getConnId(), Command.values()[req.getCmd()], req.getArgList());
      
      responseObserver.onNext(reply);
      responseObserver.onCompleted();

    }
    
    
  }
}

