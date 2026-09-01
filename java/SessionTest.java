package co.atoms.lib.net.session;

import static co.atoms.lib.net.session.testing.Asserts.assertCondition;
import static co.atoms.lib.net.session.testing.Asserts.assertElement;
import static co.atoms.lib.net.session.testing.Asserts.assertElementPresent;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.atoms.lib.net.session.proto.Message;
import co.atoms.lib.net.session.proto.Instance;
import co.atoms.lib.net.session.testing.MutableClock;
import io.grpc.stub.StreamObserver;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;

public class SessionTest {

  private static final Instant INSTANT = Instant.parse("2021-01-01T11:22:33Z");

  private static final MutableClock CLOCK = MutableClock.at(ZoneId.systemDefault(), INSTANT);
  private static final Instance INSTANCE =
      Instance.newBuilder().setId("instance-id").build();
  private static final Instance SERVER =
      Instance.newBuilder().setId("server-id").build();

  @Test
  public void streamCompletedBeforeEstablishing() {
    var deps = setupDeps(CLOCK);
    var session = deps.session;

    // Close the stream
    deps.in.onCompleted();

    assertElement(deps.terminated, true);
    assertCondition(session::isTerminated);
  }

  @Test
  public void sessionClosedBeforeEstablishing() {
    var deps = setupDeps(CLOCK);
    var session = deps.session;

    var executor = Executors.newSingleThreadExecutor();

    // awaitEstablishedOrTerminated methods should finish when session is terminated
    LinkedBlockingQueue<Boolean> terminatedSignals = new LinkedBlockingQueue<>();
    executor.submit(
        () -> {
          terminatedSignals.add(true);
          session.awaitEstablishedOrTerminated(Duration.ofMillis(300));
          terminatedSignals.add(true);
        });
    assertElement(terminatedSignals, true, "expected awaitTerminated to start");

    executor.submit(
        () -> {
          session.awaitEstablishedOrTerminated();
          terminatedSignals.add(true);
        });

    assertFalse(session.isTerminated());

    // Close the session
    deps.in.onNext(new Msg(Messages.createClosed("")));

    assertElement(deps.terminated, true);

    assertElement(terminatedSignals, true, "expected awaitEstablishedOrTerminated to finish");
    assertElement(terminatedSignals, true, "expected awaitEstablishedOrTerminated to finish");

    assertCondition(session::isTerminated);
  }

  @Test
  public void sessionExpiredBeforeEstablishing() {
    MutableClock clock = MutableClock.at(ZoneId.systemDefault(), INSTANT);

    var deps = setupDeps(clock);
    var session = deps.session;

    // Wait till session is waiting for established
    assertCondition(() -> clock.getCount() > 3);

    // Advance clock to expire the session
    clock.advance(Duration.ofSeconds(11));
    session.notifyLock();

    assertElementPresent(deps.exceptions, "expected stream to be terminated with an exception");
    assertCondition(session::isTerminated);
  }

  @Test
  public void sessionCompletedAfterEstablishing() {
    var deps = setupDeps(CLOCK);
    var session = deps.session;

    deps.establish();

    // Send a regular message from the server
    var fromMsg = new Msg("from-server");
    deps.in.onNext(fromMsg);
    assertElement(deps.toClient, fromMsg, "expected message from server");

    // Send a regular message to the server
    var toMsg = new Msg("to-server");
    session.sendAsync(toMsg);
    assertElement(deps.sent, toMsg, "expected message to server");

    // Close the stream
    deps.in.onCompleted();

    assertElement(deps.terminated, true);
    assertCondition(session::isTerminated);
  }

  @Test
  public void sessionClosedAfterEstablishing() {
    var deps = setupDeps(CLOCK);
    var session = deps.session;

    deps.establish();

    // Send a regular message from the server
    var fromMsg = new Msg("from-server");
    deps.in.onNext(fromMsg);
    assertElement(deps.toClient, fromMsg, "expected message from server");

    // Send a regular message to the server
    var toMsg = new Msg("to-server");
    session.sendAsync(toMsg);
    assertElement(deps.sent, toMsg, "expected message to server");

    // Close the session
    deps.in.onNext(new Msg(Messages.createClosed("")));

    assertElement(deps.terminated, true);
    assertCondition(session::isTerminated);
  }

  @Test
  public void sessionSendsClosedMessage() {
    var deps = setupDeps(CLOCK);
    var session = deps.session;

    deps.establish();

    // Send a closed message to the server
    session.sendClosedAsync();
    assertElement(
        deps.sent, new Msg(Messages.createClosed("")), "expected closed message to server");

    // Send a closed message with text to the server
    session.sendClosedByErrorAsync(new RuntimeException("some error"));
    assertElement(
        deps.sent,
        new Msg(Messages.createClosed("java.lang.RuntimeException: some error")),
        "expected closed message to server");

    // Close the session
    deps.in.onNext(new Msg(Messages.createClosed("")));

    assertElement(deps.terminated, true);
    assertCondition(session::isTerminated);
  }

  @Test
  public void sessionExpiredAfterEstablishing() {
    MutableClock clock = MutableClock.at(ZoneId.systemDefault(), INSTANT);

    var deps = setupDeps(clock);
    var session = deps.session;

    deps.establish();

    // Send a regular message from the server
    var fromMsg = new Msg("from-server");
    deps.in.onNext(fromMsg);
    assertElement(deps.toClient, fromMsg, "expected message from server");

    // Send a regular message to the server
    var toMsg = new Msg("to-server");
    session.sendAsync(toMsg);
    assertElement(deps.sent, toMsg, "expected message to server");

    // Advance clock to expire the session
    deps.clock.advance(Duration.ofSeconds(11));
    session.notifyLock();

    assertElementPresent(deps.exceptions);
    assertCondition(session::isTerminated);
  }

  @Test
  public void awaitForTermination() {
    MutableClock clock = MutableClock.at(ZoneId.systemDefault(), INSTANT);
    var deps = setupDeps(clock);
    var session = deps.session;

    var executor = Executors.newSingleThreadExecutor();
    LinkedBlockingQueue<Boolean> signals = new LinkedBlockingQueue<>();

    // awaitEstablishedOrTerminated should wait given duration and finish
    executor.submit(
        () -> {
          signals.add(true);
          session.awaitEstablishedOrTerminated(Duration.ofMillis(300));
          signals.add(true);
        });
    assertElement(signals, true, "expected awaitEstablishedOrTerminated to start");
    var count = new AtomicInteger(clock.getCount());

    // Wait till session is waiting for awaitEstablishedOrTerminated
    assertCondition(() -> clock.getCount() > count.get() + 2);

    // Advance clock to stop waiting
    deps.clock.advance(Duration.ofMillis(301));
    session.notifyLock();

    assertElement(signals, true, "expected awaitEstablishedOrTerminated to finish");
    assertFalse(session.isTerminated());
    assertFalse(session.isEstablished());

    // awaitTerminated should wait given duration and finish
    executor.submit(
        () -> {
          signals.add(true);
          session.awaitTerminated(Duration.ofMillis(300));
          signals.add(true);
        });
    assertElement(signals, true, "expected awaitTerminated to start");
    count.set(clock.getCount());

    // Wait till session is waiting for awaitEstablishedOrTerminated
    assertCondition(() -> clock.getCount() > count.get() + 2);

    // Advance clock to stop waiting
    deps.clock.advance(Duration.ofMillis(301));
    session.notifyLock();

    assertElement(signals, true, "expected awaitTerminated to finish");
    assertFalse(session.isTerminated());
    assertFalse(session.isEstablished());

    // awaitEstablishedOrTerminated methods should finish when session is established
    count.set(clock.getCount());
    LinkedBlockingQueue<Boolean> establishedSignals = new LinkedBlockingQueue<>();
    executor.submit(
        () -> {
          establishedSignals.add(true);
          session.awaitEstablishedOrTerminated(Duration.ofMillis(300));
          establishedSignals.add(true);
        });
    assertElement(establishedSignals, true, "expected awaitEstablishedOrTerminated to start");
    count.set(clock.getCount());

    // Wait till session is waiting for awaitEstablishedOrTerminated
    assertCondition(() -> clock.getCount() > count.get() + 2);

    executor.submit(
        () -> {
          session.awaitEstablishedOrTerminated();
          establishedSignals.add(true);
        });

    // Establish the session to stop waiting
    deps.establish();
    session.notifyLock();

    assertElement(establishedSignals, true, "expected awaitEstablishedOrTerminated to finish");
    assertElement(establishedSignals, true, "expected awaitEstablishedOrTerminated to finish");

    assertFalse(session.isTerminated());
    assertTrue(session.isEstablished());

    // awaitTerminated methods should finish when session is terminated
    count.set(clock.getCount());
    LinkedBlockingQueue<Boolean> terminatedSignals = new LinkedBlockingQueue<>();
    executor.submit(
        () -> {
          terminatedSignals.add(true);
          session.awaitTerminated(Duration.ofMillis(300));
          terminatedSignals.add(true);
        });
    assertElement(terminatedSignals, true, "expected awaitTerminated to start");
    count.set(clock.getCount());

    // Wait till session is waiting for awaitEstablishedOrTerminated
    assertCondition(() -> clock.getCount() > count.get() + 2);

    executor.submit(
        () -> {
          session.awaitTerminated();
          terminatedSignals.add(true);
        });

    assertFalse(session.isTerminated());

    // Close the session to stop waiting
    deps.in.onNext(new Msg(Messages.createClosed("")));
    session.notifyLock();

    assertElement(deps.terminated, true);
    assertCondition(session::isTerminated);

    assertElement(terminatedSignals, true, "expected awaitTerminated to finish");
    assertElement(terminatedSignals, true, "expected awaitTerminated to finish");
  }

  private static class SessionTestDeps {
    final MutableClock clock;
    // Messages sent by the session to the server
    final LinkedBlockingQueue<Msg> sent;
    // Signals when the stream is terminated
    final LinkedBlockingQueue<Boolean> terminated;
    // Exceptions thrown by the stream
    final LinkedBlockingQueue<Throwable> exceptions;
    // Messages incoming to the session from the server
    final StreamObserver<Msg> in;
    // Messages outgoing from the session to the server
    final LinkedBlockingQueue<Msg> toClient;
    final Session<Msg, Msg> session;

    SessionTestDeps(
        MutableClock clock,
        LinkedBlockingQueue<Msg> sent,
        LinkedBlockingQueue<Boolean> terminated,
        LinkedBlockingQueue<Throwable> exceptions,
        StreamObserver<Msg> in,
        LinkedBlockingQueue<Msg> toClient,
        Session<Msg, Msg> session) {
      this.clock = clock;
      this.sent = sent;
      this.terminated = terminated;
      this.exceptions = exceptions;
      this.in = in;
      this.toClient = toClient;
      this.session = session;
    }

    void establish() {
      var established = newEstablished(clock);
      in.onNext(established);
      assertElement(toClient, established, "expected established message from server");
      assertTrue(session.isEstablished());
    }
  }

  private SessionTestDeps setupDeps(MutableClock clock) {
    LinkedBlockingQueue<Msg> sent = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<Boolean> terminated = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<Throwable> exceptions = new LinkedBlockingQueue<>();
    AtomicReference<StreamObserver<Msg>> in = new AtomicReference<>();
    var out = newStreamObserver(sent, exceptions, terminated);
    LinkedBlockingQueue<Msg> toClient = new LinkedBlockingQueue<>();

    var session =
        Session.startAsync(
            clock,
            INSTANCE,
            Executors.newSingleThreadExecutor(),
            (observer) -> {
              in.set(observer); // called during startAsync, safe to assume it's set after
              return out;
            },
            Msg::getSessionMsg,
            Msg::new,
            () -> false,
            toClient::add);

    // Wait for establish
    assertElement(
        sent,
        new Msg(Messages.createEstablish(INSTANCE, session.getId())),
        "expected establish message");

    return new SessionTestDeps(clock, sent, terminated, exceptions, in.get(), toClient, session);
  }

  private static class Msg {
    final String msg;
    final @Nullable Message sessionMsg;

    private Msg(String msg, @Nullable Message sessionMsg) {
      this.msg = msg;
      this.sessionMsg = sessionMsg;
    }

    Msg(String msg) {
      this(msg, null);
    }

    Msg(@Nullable Message sessionMsg) {
      this("", sessionMsg);
    }

    static @Nullable Message getSessionMsg(Msg msg) {
      return msg.sessionMsg;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof Msg)) {
        return false;
      }
      Msg other = (Msg) obj;
      return msg.equals(other.msg) && Objects.equals(sessionMsg, other.sessionMsg);
    }

    @Override
    public int hashCode() {
      return Objects.hash(msg, sessionMsg);
    }

    @Override
    public String toString() {
      return String.format("Msg{msg=%s, sessionMsg=%s}", msg, sessionMsg);
    }
  }

  private static Msg newEstablished(Clock clock) {
    return new Msg(newEstablished(clock, SERVER));
  }

  public static Message newEstablished(Clock clock, Instance server) {
    return Message.newBuilder()
        .setEstablished(
            Message.Established.newBuilder()
                .setTtl(ProtobufTime.toTimestamp(Instant.now(clock)))
                .setServer(server)
                .build())
        .build();
  }

  private static <T> StreamObserver<T> newStreamObserver(
      LinkedBlockingQueue<T> sent,
      LinkedBlockingQueue<Throwable> exceptions,
      LinkedBlockingQueue<Boolean> terminated) {
    return new StreamObserver<>() {
      @Override
      public void onNext(T value) {
        sent.add(value);
      }

      @Override
      public void onError(Throwable t) {
        exceptions.add(t);
      }

      @Override
      public void onCompleted() {
        terminated.add(true);
      }
    };
  }
}
