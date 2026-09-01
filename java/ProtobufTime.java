package co.atoms.lib.net.session;

import com.google.protobuf.Timestamp;
import java.time.Instant;

class ProtobufTime {
  private ProtobufTime() {}

  public static Instant toInstant(final Timestamp timestamp) {
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }

  public static Timestamp toTimestamp(final Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }
}
