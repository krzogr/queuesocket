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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketImpl;
import java.net.SocketOptions;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * The actual implementation of queue socket.
 * 
 * QueueSocket uses queue streams for input and output and it enables inter-process communication without the network.
 **/
public class QueueSocket extends SocketImpl {
  /**
   * Maximum time the socket will try to establish connection in a single iteration.
   */
  private static final long ITERATION_TIME_MILLIS = 30;

  /**
   * The time socket will wait between subsequent iterations to establish the connection.
   */
  private static final long SLEEP_TIME_MILLIS = 10;

  /** Container for socket options (not used currently). */
  private final Map<Integer, Object> options = new HashMap<Integer, Object>();

  /**
   * Contains internal state associated with the socket.
   */
  private final QueueSocketState state = new QueueSocketState();

  /** Creates disconnected socket. */
  public QueueSocket() {
    initOptions();
  }

  /**
   * Creates connected socket associated with specified streams.
   * 
   * @param input Input stream.
   * @param output Output stream.
   * @throws IOException Thrown if socket initialization failed.
   */
  public QueueSocket(final QueueStream input, final QueueStream output) throws IOException {
    initOptions();
    state.init(input, output);
  }

  private void initOptions() {
    options.put(SocketOptions.SO_LINGER, Integer.valueOf(0));
    options.put(SocketOptions.TCP_NODELAY, Boolean.FALSE);
    options.put(SocketOptions.SO_TIMEOUT, Integer.valueOf(0));
    options.put(SocketOptions.SO_SNDBUF, Integer.valueOf(0));
    options.put(SocketOptions.SO_RCVBUF, Integer.valueOf(0));
    options.put(SocketOptions.SO_KEEPALIVE, Boolean.FALSE);
  }

  @Override
  public void setOption(final int optID, final Object value) throws SocketException {
    options.put(Integer.valueOf(optID), value);
  }

  @Override
  public Object getOption(final int optID) throws SocketException {
    return options.get(Integer.valueOf(optID));
  }

  @Override
  protected void create(final boolean stream) throws IOException {
    // Empty
  }

  @Override
  protected void connect(final String host, final int port) throws IOException {
    doConnect(host, port, manager().getMaxConnectTimeMillis());
  }

  @Override
  protected void connect(final InetAddress address, final int port) throws IOException {
    doConnect(address.getHostName(), port, manager().getMaxConnectTimeMillis());
  }

  @Override
  protected void connect(final SocketAddress address, final int timeout) throws IOException {
    InetSocketAddress inetAddress = (InetSocketAddress) address;
    long timeoutMillis = timeout > 0 ? timeout : manager().getMaxConnectTimeMillis();

    doConnect(inetAddress.getHostName(), inetAddress.getPort(), timeoutMillis);
  }

  private void doConnect(final String host, final int port, final long timeoutMillis) throws IOException {
    Objects.requireNonNull(host);

    QueueStream output = new QueueStream();
    QueueStream input = connectClientSocket(host, port, output, timeoutMillis, MILLISECONDS);

    state.init(input, output);
  }

  @Override
  protected void bind(final InetAddress host, final int port) throws IOException {
    super.port = port;
    super.localport = port;

    if (port == 0) {
      super.localport = manager().getNextEphemeralPort();
    }
  }

  @Override
  protected void listen(final int backlog) throws IOException {
    // Empty
  }

  @Override
  protected void accept(final SocketImpl actualSocket) throws IOException {
    Objects.requireNonNull(actualSocket);

    final QueueSocket socket = (QueueSocket) actualSocket;
    socket.port = super.getPort();
    socket.localport = manager().getNextEphemeralPort();

    QueueStream output = new QueueStream();
    QueueStream input = connectServerSocket("localhost", socket.port, output);

    socket.state.init(input, output);
  }

  @Override
  protected InputStream getInputStream() throws IOException {
    return state.getInputStream();
  }

  @Override
  protected OutputStream getOutputStream() throws IOException {
    return state.getOutputStream();
  }

  @Override
  protected int available() throws IOException {
    return state.getInputStream().available();
  }

  @Override
  protected void close() throws IOException {
    state.close();
  }

  @Override
  protected void sendUrgentData(final int data) throws IOException {
    throw new UnsupportedOperationException();
  }

  private QueueStream connectServerSocket(final String host, final int port, final QueueStream output)
      throws IOException {
    return getInputStream(Long.MAX_VALUE, MILLISECONDS, () -> exchangeServerSocketStream(host, port, output));
  }

  private QueueStream connectClientSocket(final String host, final int port, final QueueStream output,
      final long maxTime, final TimeUnit maxTimeUnit) throws IOException {
    return getInputStream(maxTime, maxTimeUnit, () -> exchangeClientSocketStream(host, port, output));
  }

  private QueueStream exchangeServerSocketStream(final String host, final int port, final QueueStream output) {
    return manager().exchangeServerSocketStream(host, port, output, ITERATION_TIME_MILLIS, MILLISECONDS);
  }

  private QueueStream exchangeClientSocketStream(final String host, final int port, final QueueStream output) {
    return manager().exchangeClientSocketStream(host, port, output, ITERATION_TIME_MILLIS, MILLISECONDS);
  }

  /**
   * Runs the specified operation until socket input stream is obtained or the specified time expires.
   * 
   * @param maxTime Maximum time to spend.
   * @param maxTimeUnit Time unit.
   * @param operation Operation used to retrieve input stream.
   * @return Input stream to use or NULL if the specified time expires.
   * @throws IOException
   */
  private QueueStream getInputStream(final long maxTime, final TimeUnit maxTimeUnit,
      final Supplier<QueueStream> operation) {
    long startTimeMillis = System.currentTimeMillis();
    long maxTimeMillis = maxTimeUnit.toMillis(maxTime);

    long timeSpentMillis = 0;

    while (state.isSocketOpen() && isThreadActive() && timeSpentMillis < maxTimeMillis) {
      // Run an iteration to exchange streams and establish connection
      QueueStream result = operation.get();

      if (result != null) {
        return result;
      } else {
        QueueSocketUtils.delayQuietly(SLEEP_TIME_MILLIS, TimeUnit.MILLISECONDS);
        timeSpentMillis = System.currentTimeMillis() - startTimeMillis;
      }
    }

    return null;
  }

  private QueueSocketManager manager() {
    return QueueSocketManager.getInstance();
  }

  private boolean isThreadActive() {
    return !Thread.currentThread().isInterrupted();
  }
}
