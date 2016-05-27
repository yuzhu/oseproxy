package ucb.oseproxy.util;

import com.google.protobuf.DescriptorProtos;


import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import java.util.HashMap;

public class DynamicSchema {
  private DescriptorProtos.DescriptorProto.Builder desBuilder;
  private int i = 1;
  private DescriptorProtos.DescriptorProto dsc = null;

  public DynamicSchema() {
    desBuilder = DescriptorProtos.DescriptorProto.newBuilder();
    i = 1;
  }
  
  // Read Only Schema
  public DynamicSchema(DescriptorProtos.DescriptorProto dsc) {
    this.dsc = dsc;
  }
  public <T> void addField(String fieldName,  
      DescriptorProtos.FieldDescriptorProto.Type type) {
    DescriptorProtos.FieldDescriptorProto.Builder fd1Builder = DescriptorProtos.FieldDescriptorProto
        .newBuilder().setName(fieldName).setNumber(i++).setType(type);
    desBuilder.addField(fd1Builder.build());
  }

  public void setName(String schemaName) {
    desBuilder.setName("MT" + schemaName.replace('-', 'Z'));
  }

  public DescriptorProtos.DescriptorProto getDescProto() {
    if (dsc == null)
      dsc = desBuilder.build();
    return dsc;
  }

  public static Descriptors.Descriptor getDesc(DescriptorProtos.DescriptorProto descp) {
    DescriptorProtos.FileDescriptorProto fileDescP =
        DescriptorProtos.FileDescriptorProto.newBuilder().addMessageType(descp).build();

    Descriptors.FileDescriptor[] fileDescs = new Descriptors.FileDescriptor[0];
    // ? 
    Descriptors.FileDescriptor dynamicDescriptor;
    try {
      dynamicDescriptor = Descriptors.FileDescriptor.buildFrom(fileDescP, fileDescs);
    } catch (DescriptorValidationException e) {
      System.out.println("Exception converting descriptors");
      e.printStackTrace();
      return null;
    }
    Descriptors.Descriptor msgDescriptor = dynamicDescriptor.findMessageTypeByName(descp.getName());
    return msgDescriptor;
    
  }
  public Descriptors.Descriptor getDescriptor() {
    DescriptorProtos.DescriptorProto descp = this.getDescProto();
    System.out.println (descp.toString());
    return DynamicSchema.getDesc(descp);
  }
  
  public DynamicMessage.Builder newMessageBuilder(String msgTypeName) {
    this.setName(msgTypeName);
    DescriptorProtos.DescriptorProto dsc = desBuilder.build();

    DescriptorProtos.FileDescriptorProto fileDescP =
        DescriptorProtos.FileDescriptorProto.newBuilder().addMessageType(dsc).build();

    Descriptors.FileDescriptor[] fileDescs = new Descriptors.FileDescriptor[0];
    // ? 
    Descriptors.FileDescriptor dynamicDescriptor;
    try {
      dynamicDescriptor = Descriptors.FileDescriptor.buildFrom(fileDescP, fileDescs);
    } catch (DescriptorValidationException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    }
    Descriptors.Descriptor msgDescriptor = dynamicDescriptor.findMessageTypeByName(msgTypeName);
    DynamicMessage.Builder dmBuilder = DynamicMessage.newBuilder(msgDescriptor);
    return dmBuilder;
  }
}
