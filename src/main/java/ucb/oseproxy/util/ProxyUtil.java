package ucb.oseproxy.util;

import java.util.UUID;

public class ProxyUtil {
  public static String randomId() {
    // Static factory to retrieve a type 4 (pseudo randomly generated) UUID
    String uuid = UUID.randomUUID().toString();
    return uuid;
  }
  
  public static String randomIdFromString(String code) {
    // Static factory to retrieve a type 4 (pseudo randomly generated) UUID
    String uuid = UUID.nameUUIDFromBytes(code.getBytes()).toString();
    return uuid;
  }
}
