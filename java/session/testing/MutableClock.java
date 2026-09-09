package co.atoms.lib.net.session.testing;

import static java.util.Objects.requireNonNull;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAmount;

/** {@link Clock} implementation for testing. Allows modifying the current time. */
public class MutableClock extends Clock {

  private final ZoneId zoneId;
  private volatile Instant instant;

  private final Object lock = new Object();
  private int count = 0;

  private MutableClock(ZoneId zoneId, Instant currentInstant) {
    this.zoneId = requireNonNull(zoneId);
    this.instant = requireNonNull(currentInstant);
  }

  public static MutableClock at(Instant instant) {
    return new MutableClock(ZoneOffset.UTC, instant);
  }

  public static MutableClock at(ZoneId zoneId, Instant instant) {
    return new MutableClock(zoneId, instant);
  }

  @Override
  public ZoneId getZone() {
    return zoneId;
  }

  @Override
  public Clock withZone(ZoneId zone) {
    return MutableClock.at(zone, instant);
  }

  @Override
  public Instant instant() {
    synchronized (lock) {
      count++;
      return instant;
    }
  }

  public int getCount() {
    synchronized (lock) {
      return count;
    }
  }

  public MutableClock advance(TemporalAmount temporalAmount) {
    synchronized (lock) {
      instant = instant.plus(temporalAmount);
    }
    return this;
  }

  public void setInstant(Instant newInstant) {
    this.instant = requireNonNull(newInstant);
  }
}
