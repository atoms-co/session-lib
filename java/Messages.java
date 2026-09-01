package co.atoms.lib.net.session;

import co.atoms.lib.net.session.proto.Instance;
import co.atoms.lib.net.session.proto.Message;
import java.time.Instant;

public class Messages {
  private Messages() {}

  public static Message createEstablish(Instance client, String id) {
    return Message.newBuilder()
        .setEstablish(Message.Establish.newBuilder().setClient(client).setId(id))
        .build();
  }

  public static Message createHeartbeat(Instant timestamp) {
    return Message.newBuilder()
        .setHeartbeat(Message.Heartbeat.newBuilder().setNow(ProtobufTime.toTimestamp(timestamp)))
        .build();
  }

  public static Message createClosed(String error) {
    return Message.newBuilder().setClosed(Message.Closed.newBuilder().setError(error)).build();
  }
}
