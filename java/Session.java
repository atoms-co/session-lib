package co.atoms.lib.net.session;

import co.atoms.lib.net.session.proto.Instance;
import co.atoms.lib.net.session.proto.Message;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.TextFormat;
import io.grpc.stub.StreamObserver;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Session maintains a bidirectional stream of messages between a client and a server.
 *
 * <p>Server and client maintain constant connectivity on both sides by sending periodic heartbeat
 * messages. If connectivity is lost, the session is terminated without re-connecting. Re-connection
 * must be handled by the client.
 *
 * <p>Session is started asynchronously and runs until terminated either by an error or explicitly
 * by the server or by the client. The session is considered established when it receives an
 * establish message from the server.
 *
 * <p>Session messages contain session control messages and client messages (exact structure is
 * defined by the client).
 *
 * <p>Server messages are delivered to the client using a callback function. Messages from the
 * client are sent in FIFO order; they are stored in memory until they are processed.
 *
 * @param <REQ> Type of messages sent by the server
 * @param <RESP> Type of messages sent by the client
 */
public class Session<REQ, RESP> {

  /** Interface for a gRPC streaming service method. */
  @FunctionalInterface
  public interface ServiceMethod<REQ, RESP> {
    StreamObserver<RESP> call(StreamObserver<REQ> observer);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(Session.class);

  // Duration to wait for operation while maintaining heartbeat. It's lower than heartbeat duration
  // to prevent losing heartbeat when JVM is slow
  private static final Duration LOCK_DURATION = Duration.ofSeconds(1);
  // Server expects heartbeats to be sent every 5 seconds.
  private static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(5);
  // Server replies to every heartbeat with a heartbeat ack. If no ack is received for 10 seconds,
  // session is terminated.
  private static final Duration EXPIRATION_TIMEOUT = Duration.ofSeconds(10);

  private final Clock clock;
  private final String id;
  private final Instance client;

  private final Function<REQ, Message> unwrapReq;
  private final Function<Message, RESP> wrapResp;
  private final Consumer<REQ> toClient;
  private final LinkedBlockingQueue<RESP> toServer = new LinkedBlockingQueue<>();
  private final BooleanSupplier verbose;

  private volatile @Nullable Instance server;
  // last time we received a periodic status message
  private Instant lastStatus;

  private volatile boolean established = false;
  private volatile boolean terminated = false;
  private final Object lock = new Object();

  private Session(
      Clock clock,
      Instance client,
      Function<REQ, Message> unwrapReq,
      Function<Message, RESP> wrapResp,
      BooleanSupplier verbose,
      Consumer<REQ> toClient) {
    this.clock = clock;
    this.id = UUID.randomUUID().toString();
    this.client = client;
    this.unwrapReq = unwrapReq;
    this.wrapResp = wrapResp;
    this.toClient = toClient;
    this.verbose = verbose;
    this.server = null;
    this.lastStatus = Instant.now(clock);
  }

  /**
   * Starts a new session asynchronously. The session is initiated by calling the given service
   * method.
   *
   * @param instance Describes the client instance
   * @param executor Executor service to run the session maintenance loop
   * @param method Service method to call to initiate the session
   * @param unwrapReq Function to unwrap session control messages from the server messages. Returns
   *     null if not session control message.
   * @param wrapResp Function to wrap session control messages into the client messages
   * @param verbose Supplier for verbose logging
   * @param toClient Consumer for all messages sent from the server
   * @param <REQ> Type of messages sent by the server
   * @param <RESP> Type of messages sent by the client
   */
  public static <REQ, RESP> Session<REQ, RESP> startAsync(
      Clock clock,
      Instance instance,
      ExecutorService executor,
      ServiceMethod<REQ, RESP> method,
      Function<REQ, Message> unwrapReq,
      Function<Message, RESP> wrapResp,
      BooleanSupplier verbose,
      Consumer<REQ> toClient) {
    var session = new Session<REQ, RESP>(clock, instance, unwrapReq, wrapResp, verbose, toClient);

    StreamObserver<RESP> outgoing =
        method.call(
            new StreamObserver<>() {
              @Override
              public void onNext(REQ msg) {
                session.processIncomingMessage(msg);
              }

              @Override
              public void onError(Throwable t) {
                session.onStreamError(t);
              }

              @Override
              public void onCompleted() {
                session.onStreamCompleted();
              }
            });

    executor.submit(() -> session.process(outgoing));
    return session;
  }

  private void process(StreamObserver<RESP> outgoing) {
    try {
      LOGGER.info("Starting session {}", this);
      outgoing.onNext(wrapResp.apply(Messages.createEstablish(client, id)));

      var lastHeartbeat = Instant.now(clock);
      lastStatus = lastHeartbeat;

      // Wait for establish message. Check status in case established message doesn't arrive in
      // time.
      while (!terminated && !established) {
        awaitEstablishedOrTerminated(LOCK_DURATION);

        var now = Instant.now(clock);
        assertStatusNotExpired(now);
      }

      // If session is terminated before established, close the stream.
      if (terminated) {
        LOGGER.info(
            "Session {} is terminated before established message is received. Terminating", this);
        outgoing.onCompleted();
        return;
      }

      // Maintain heartbeat before checking for messages to send.
      lastHeartbeat = updateHeartbeatIfNeeded(outgoing, Instant.now(clock), lastHeartbeat);

      while (!terminated) {
        var msg = toServer.poll(LOCK_DURATION.toMillis(), TimeUnit.MILLISECONDS);
        if (msg != null) {
          outgoing.onNext(msg);
        }

        var now = Instant.now(clock);
        lastHeartbeat = updateHeartbeatIfNeeded(outgoing, now, lastHeartbeat);
        assertStatusNotExpired(now);
      }

      outgoing.onCompleted();
    } catch (Throwable t) {
      LOGGER.warn("Unexpected error while handling session {}", this, t);
      outgoing.onError(t);
      terminate();
    }
    LOGGER.info("Terminated session {}", this);
  }

  private Instant updateHeartbeatIfNeeded(
      StreamObserver<RESP> outgoing, Instant now, Instant lastHeartbeat) {
    if (lastHeartbeat.plus(HEARTBEAT_INTERVAL).isBefore(now)) {
      outgoing.onNext(wrapResp.apply(Messages.createHeartbeat(now)));
      return now;
    }
    return lastHeartbeat;
  }

  private void assertStatusNotExpired(Instant now) throws Exception {
    if (lastStatus.plus(EXPIRATION_TIMEOUT).isBefore(now)) {
      LOGGER.warn(
          "Session {} did not received status response from server in {}. Closing",
          this,
          EXPIRATION_TIMEOUT);
      throw new Exception(
          String.format("no status responses from server for %s", EXPIRATION_TIMEOUT));
    }
  }

  private void onStreamError(Throwable t) {
    LOGGER.warn("Unexpected stream error for session {}", this, t);
    terminate();
  }

  private void onStreamCompleted() {
    LOGGER.info("Stream for session {} completed", this);
    terminate();
  }

  private void processIncomingMessage(REQ req) {
    if (verbose.getAsBoolean()) {
      LOGGER.info("Session {} received message from server {}", this, req);
    }

    // Pass message to the client
    toClient.accept(req);

    var msg = unwrapReq.apply(req);

    // Not a session control message
    if (msg == null) {
      if (!established) {
        LOGGER.warn(
            "Session {} received unexpected message from server before established: {}. Ignoring",
            this,
            req);
      }
      return;
    }

    switch (msg.getRequestCase()) {
      case ESTABLISHED:
        if (established) {
          LOGGER.error(
              "Session {} received unexpected established message {}. Terminating", this, msg);
          terminate();
          return;
        }

        lastStatus = ProtobufTime.toInstant(msg.getEstablished().getTtl());
        markEstablished(msg.getEstablished().getServer());
        break;
      case ACK:
        if (!established) {
          LOGGER.error(
              "Session {} received unexpected message from server before established: {}. Ignoring",
              this,
              msg);
        }
        lastStatus = ProtobufTime.toInstant(msg.getAck().getTtl());
        break;
      case CLOSED:
        LOGGER.info("Session {} closed by server request", this);
        terminate();
        break;
    }
  }

  private void markEstablished(Instance server) {
    this.server = server;
    LOGGER.info("Established session {}", this);
    established = true;
    synchronized (lock) {
      lock.notifyAll();
    }
  }

  private void terminate() {
    if (terminated) {
      return;
    }
    LOGGER.info("Session {} terminated", this);
    terminated = true;
    synchronized (lock) {
      lock.notifyAll();
    }
  }

  public String getId() {
    return id;
  }

  /**
   * @return true if the session is established. Session is established after receiving an
   *     established message from the server.
   */
  public boolean isEstablished() {
    return established;
  }

  /**
   * Wait for the session to be established or terminated. Session can be terminated while waiting
   * for the established message from the server. Clients must check session status after calling
   * this method.
   */
  public void awaitEstablishedOrTerminated() {
    try {
      synchronized (lock) {
        while (!established && !terminated) {
          lock.wait();
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  /**
   * Wait for the session to be established or terminated for no more than the given duration.
   * Session can be terminated while waiting for the established message from the server. Clients
   * must check session status after calling this method.
   */
  public void awaitEstablishedOrTerminated(Duration duration) {
    var now = Instant.now(clock);
    try {
      synchronized (lock) {
        while (!established && !terminated && Instant.now(clock).isBefore(now.plus(duration))) {
          lock.wait(duration.toMillis());
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  /**
   * @return true if the session is terminated. Session is terminated after receiving a closed
   *     message from the server or when an error occurs.
   */
  public boolean isTerminated() {
    return terminated;
  }

  /**
   * Wait for the session to be terminated. This should be used after the session is established.
   */
  public void awaitTerminated() {
    try {
      synchronized (lock) {
        while (!terminated) {
          lock.wait();
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  /**
   * Wait for the session to be terminated for no more than the given duration. This should be used
   * after the session is established.
   */
  public void awaitTerminated(Duration duration) {
    var now = Instant.now(clock);
    try {
      synchronized (lock) {
        while (!terminated && Instant.now(clock).isBefore(now.plus(duration))) {
          lock.wait(duration.toMillis());
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  /** Send a message to the server asynchronously. */
  public void sendAsync(RESP msg) {
    toServer.add(msg);
  }

  /** Send a message to close the session to the server due to an error. */
  public void sendClosedByErrorAsync(Throwable t) {
    toServer.add(wrapResp.apply(Messages.createClosed(t.toString())));
  }

  /**
   * Send a message to close the session to the server under normal circumstances (e.g. shutting
   * down).
   */
  public void sendClosedAsync() {
    LOGGER.info("Closing session {}", this);
    toServer.add(wrapResp.apply(Messages.createClosed("")));
  }

  public String toString() {
    return "Session{id=%s, client=%s, server=%s, established=%s, terminated=%s}"
        .formatted(
            id,
            TextFormat.printer().shortDebugString(client),
            server == null ? "<none>" : TextFormat.printer().shortDebugString(server),
            established,
            terminated);
  }

  @VisibleForTesting
  void notifyLock() {
    synchronized (lock) {
      lock.notifyAll();
    }
  }
}
