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

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Endpoint associated with a single queue socket which enables an application to communicate with the socket.
 */
public class QueueSocketEndpoint {
  /**
   * Server which the socket is connected to.
   */
  private final String server;

  /**
   * Port which the socket is connected to.
   */
  private final int port;

  /**
   * Input stream which enables to read data sent by the socket.
   */
  private final QueueStream input;

  /**
   * Output stream which enables to write data to the socket.
   */
  private final QueueStream output;

  /**
   * Creates queue socket endpoint.
   * 
   * @param server Server which the socket is connected to.
   * @param port Port which the socket is connected to.
   * @param input Input stream which enables to read data sent by the socket.
   * @param output Output stream which enables to write data to the socket.
   */
  public QueueSocketEndpoint(final String server, final int port, final QueueStream input, final QueueStream output) {
    Objects.requireNonNull(server);
    Objects.requireNonNull(input);
    Objects.requireNonNull(output);

    this.server = server;
    this.port = port;
    this.input = input;
    this.output = output;
  }

  public String getServer() {
    return server;
  }

  public int getPort() {
    return port;
  }

  /**
   * Returns the next byte buffer which was received from the socket or NULL if the specified time expired.
   * 
   * @param maxTime Maximum time to spend waiting for the data.
   * @param maxTimeUnit Time unit.
   * @return next byte buffer which was received from the socket or NULL if the specified time expired.
   */
  public byte[] getNextReceivedBuffer(long maxTime, TimeUnit maxTimeUnit) {
    return input.getNextBuffer(maxTime, maxTimeUnit);
  }

  /**
   * Returns the next byte buffer as String which received from the socket or NULL if the specified time expired.
   * 
   * @param maxTime Maximum time to spend waiting for the data.
   * @param maxTimeUnit Time unit.
   * @return next byte buffer as String which was received from the socket or NULL if the specified time expired.
   */
  public String getNextReceivedString(long maxTime, TimeUnit maxTimeUnit) {
    byte[] bytes = getNextReceivedBuffer(maxTime, maxTimeUnit);
    return bytes != null ? QueueSocketUtils.getString(bytes) : null;
  }

  /**
   * Returns {@literal exactCount} bytes which were received from the socket or NULL if the specified time expired.
   * 
   * @param exactCount Number of bytes to retrieve.
   * @param maxTime Maximum time to spend waiting for the data.
   * @param maxTimeUnit Time unit.
   * @return The requested number of bytes which were received from the socket or NULL if the specified time expired.
   */
  public byte[] getReceivedBytes(int exactCount, long maxTime, TimeUnit maxTimeUnit) {
    return input.getNextBytes(exactCount, true, maxTime, maxTimeUnit);
  }

  /**
   * Returns up to {@literal maxCount} bytes which were received from the socket or NULL if the specified time expired.
   * 
   * @param exactCount Maximum number of bytes to retrieve.
   * @param maxTime Maximum time to spend waiting for the data.
   * @param maxTimeUnit Time unit.
   * @return The available bytes which were received from the socket or NULL if the specified time expired.
   */
  public byte[] getAvailableBytes(int maxCount, long maxTime, TimeUnit maxTimeUnit) {
    return input.getNextBytes(maxCount, false, maxTime, maxTimeUnit);
  }

  /**
   * Sends the specified bytes to the socket.
   * 
   * @param msg Bytes to send.
   */
  public void sendMessage(final byte[] msg) {
    output.sendBytes(msg);
  }

  /**
   * Sends the bytes of the specified string to the socket.
   * 
   * @param msg String with bytes to send.
   */
  public void sendMessage(String msg) {
    output.sendBytes(msg);
  }

}
