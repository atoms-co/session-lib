package co.atoms.lib.net.session;

import com.google.common.base.Strings;

/** Location for component observability. Can be used for debugging. */
public record Location(String region, String node) {
  public static final Location LOCAL = fromEnv();
  public static final Location UNKNOWN = new Location("", "");

  public static Location create(String region, String node) {
    return new Location(region, node);
  }

  private static Location fromEnv() {
    String region = System.getenv("APP_REGION");
    if (Strings.isNullOrEmpty(region)) {
      region = "global";
    }
    String pod = System.getenv("HOSTNAME");
    if (Strings.isNullOrEmpty(pod)) {
      pod = "localhost";
    }
    return create(region, pod);
  }

  public static Location fromProto(co.atoms.lib.net.session.proto.Location location) {
    return create(location.getRegion(), location.getNode());
  }

  public co.atoms.lib.net.session.proto.Location toProto() {
    return co.atoms.lib.net.session.proto.Location.newBuilder()
        .setRegion(region)
        .setNode(node)
        .build();
  }

  @Override
  public String toString() {
    return String.format("%s/%s", region, node);
  }
}
