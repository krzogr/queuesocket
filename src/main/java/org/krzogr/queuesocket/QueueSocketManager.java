/*
 * Copyright (C) 2015 krzogr (krzogr@gmail.com)
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.krzogr.queuesocket;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Helper singleton which manages queue sockets and enables them to connect with each other.
 * 
 * This class also enables an application to connect to queue sockets through endpoints.
 */
public class QueueSocketManager {
  private static final QueueSocketManager INSTANCE = new QueueSocketManager();

  private static final int INIT_EPHEMERAL_PORT = 49152;

  /**
   * Wait time used when establishing connection between two sockets.
   */
  private static final int SLEEP_TIME_MILLIS = 10;

  /**
   * Localhost IP which will be resolved to "localhost" string.
   */
  private static final String LOCALHOST_IP1 = "0.0.0.0";

  /**
   * Localhost IP which will be resolved to "localhost" string.
   */
  private static final String LOCALHOST_IP2 = "127.0.0.1";

  private static final String LOCALHOST = "localhost";

  /**
   * Generator used to generate ephemeral portS when needed.
   */
  private AtomicInteger ephemeralPort = new AtomicInteger(INIT_EPHEMERAL_PORT);

  /**
   * Map which contains exchangers associated with individual sockets.
   * 
   * Exchangers are used to swap input and output streams between sockets so that they can communicate with each other.
   */
  private ConcurrentMap<String, Exchanger<QueueStreamSlot>> exchangers = new ConcurrentHashMap<String, Exchanger<QueueStreamSlot>>();

  private AtomicLong maxConnectTimeMillis = new AtomicLong(SECONDS.toMillis(1));

  /**
   * Internal type which defines socket type. Used during connection handshake to ensure that only socket of different
   * types can connect with each other.
   */
  static enum QueueSocketType {
    Client, Server
  }

  /**
   * Slot which is exchanged during connection handshake.
   */
  static class QueueStreamSlot {
    private final QueueSocketType type;
    private final QueueStream stream;

    public QueueStreamSlot(final QueueSocketType type, final QueueStream stream) {
      this.type = type;
      this.stream = stream;
    }

    public QueueSocketType getType() {
      return type;
    }

    public QueueStream getStream() {
      return stream;
    }
  }

  public static QueueSocketManager getInstance() {
    return INSTANCE;
  }

  /**
   * Returns maximum time in milliseconds queue sockets will try to connect before giving up.
   * 
   * @return Maximum time in milliseconds queue sockets will try to connect before giving up.
   */
  public long getMaxConnectTimeMillis() {
    return maxConnectTimeMillis.get();
  }

  /**
   * Sets the maximum time in milliseconds queue sockets will try to connect before giving up.
   * 
   * @param value Maximum time in milliseconds queue sockets will try to connect before giving up.
   */
  public void setMaxConnectTimeMillis(final long value) {
    maxConnectTimeMillis.set(value);
  }

  /**
   * Removes all existing pending socket connections and reinitializes the manager.
   */
  public void reset() {
    ephemeralPort.set(INIT_EPHEMERAL_PORT);
    exchangers.clear();
  }

  /**
   * Should be called by queue client socket to establish connection with the server.
   * 
   * @param server Server which the socket connects to.
   * @param port Port which the socket connects to.
   * @param output Output stream which the socket will write to.
   * @param maxTime Maximum time to spend for the connection.
   * @param maxTimeUnit Time unit.
   * @return Input stream when which the socket can read from if the connection has been established. Null if the
   *         specified time expired.
   */
  QueueStream exchangeClientSocketStream(final String server, final int port, final QueueStream output,
      final long maxTime, final TimeUnit maxTimeUnit) {
    return exchangeStream(server, port, output, QueueSocketType.Client, maxTime, maxTimeUnit);
  }

  /**
   * Should be called by queue server socket to establish connection with the client.
   * 
   * @param server Server which the socket listens on.
   * @param port Port which the socket listens on.
   * @param output Output stream which the socket will write to.
   * @param maxTime Maximum time to spend for the connection.
   * @param maxTimeUnit Time unit.
   * @return Input stream when which the socket can read from if the connection has been established. Null if the
   *         specified time expired.
   */
  QueueStream exchangeServerSocketStream(final String server, final int port, final QueueStream output,
      final long maxTime, final TimeUnit maxTimeUnit) {
    return exchangeStream(server, port, output, QueueSocketType.Server, maxTime, maxTimeUnit);
  }

  /**
   * Should be called by the application which wishes to connect to the listening queue server socket.
   * 
   * @param server Server which the socket listens on.
   * @param port Port which the socket listens on.
   * @param maxTime Maximum time to spend for the connection.
   * @param maxTimeUnit Time unit.
   * @return Endpoint which the application can use to communicate with the socket if the connection has been
   *         established. Null if the specified time expired.
   */
  public QueueSocketEndpoint connect(final String server, final int port, final long maxTime, final TimeUnit maxTimeUnit) {
    QueueStream output = new QueueStream();
    QueueStream input = exchangeStream(server, port, output, QueueSocketType.Client, maxTime, maxTimeUnit);
    return input != null ? new QueueSocketEndpoint(server, port, input, output) : null;
  }

  /**
   * Should be called by the application which wishes to accept connection from queue client socket.
   * 
   * @param server Server which connection should be accepted for.
   * @param port Port which the connection should be accepted for.
   * @param maxTime Maximum time to spend for the connection.
   * @param maxTimeUnit Time unit.
   * @return Endpoint which the application can use to communicate with the socket if the connection has been
   *         established. Null if the specified time expired.
   */
  public QueueSocketEndpoint accept(final String server, final int port, final long maxTime, final TimeUnit maxTimeUnit) {
    QueueStream output = new QueueStream();
    QueueStream input = exchangeStream(server, port, output, QueueSocketType.Server, maxTime, maxTimeUnit);
    return input != null ? new QueueSocketEndpoint(server, port, input, output) : null;
  }

  /**
   * Retrieves the next ephemeral port for the queue server socket.
   * 
   * @return Next ephemeral port for the queue server socket.
   */
  int getNextEphemeralPort() {
    return ephemeralPort.incrementAndGet();
  }

  private String getKey(final String server, final int port) {
    String serverStr = server;
    if (serverStr.equals(LOCALHOST_IP1) || serverStr.equals(LOCALHOST_IP2)) {
      serverStr = LOCALHOST;
    }

    return (serverStr + ":" + port).toUpperCase(Locale.ROOT);
  }

  /**
   * Establishes the connection between two queue sockets attempting to connect to the same server and port.
   * 
   * Connection is established when input and output streams between two sockets are exchanged.
   * 
   * @param server Server which sockets connect to.
   * @param port Port which sockets connect to.
   * @param output Output stream which the caller will use to write the data to.
   * @param type Type of socket which is connecting.
   * @param maxTime Maximum time to wait for the connection.
   * @param maxTimeUnit Time unit.
   * @return Input stream which the caller will use to read data from.
   */
  private QueueStream exchangeStream(final String server, final int port, final QueueStream output,
      final QueueSocketType type, final long maxTime, final TimeUnit maxTimeUnit) {

    Exchanger<QueueStreamSlot> exchanger = exchangers.computeIfAbsent(getKey(server, port),
        (s) -> new Exchanger<QueueStreamSlot>());

    try {
      QueueStreamSlot mySlot = new QueueStreamSlot(type, output);
      QueueStreamSlot otherSlot = exchanger.exchange(mySlot, maxTimeUnit.toMillis(maxTime), MILLISECONDS);

      if (mySlot.getType() != otherSlot.getType()) {
        // Only sockets of different types can connect with each other (Client-Server)
        return otherSlot.getStream();
      } else {
        return null;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    } catch (TimeoutException e) {
      return null;
    }
  }
}
