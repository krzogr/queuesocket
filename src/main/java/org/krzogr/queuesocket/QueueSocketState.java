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
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.ConnectException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Helper class to keep internal state associated with queue socket.
 */
class QueueSocketState {
  private final AtomicBoolean open = new AtomicBoolean(true);

  private InputStream inputStream;

  private OutputStream outputStream;

  boolean isSocketOpen() {
    return open.get();
  }

  boolean isSocketClosed() {
    return !isSocketOpen();
  }

  synchronized void init(final QueueStream input, final QueueStream output) throws IOException {
    Objects.requireNonNull(output);

    if (isSocketClosed()) {
      throw socketClosedException();
    } else if (input == null) {
      throw initException();
    } else {
      inputStream = input.getInputStream();
      outputStream = output.getOutputStream();
    }
  }

  synchronized void close() {
    open.set(false);
    cleanup();
  }

  synchronized InputStream getInputStream() throws IOException {
    if (isSocketClosed()) {
      throw socketClosedException();
    } else if (inputStream == null) {
      throw socketDisconnectedException();
    } else {
      return inputStream;
    }
  }

  synchronized OutputStream getOutputStream() throws IOException {
    if (isSocketClosed()) {
      throw socketClosedException();
    } else if (inputStream == null) {
      throw socketDisconnectedException();
    } else {
      return outputStream;
    }
  }

  private void cleanup() {
    QueueSocketUtils.closeQuietly(inputStream);
    QueueSocketUtils.closeQuietly(outputStream);

    inputStream = null;
    outputStream = null;
  }

  private IOException socketClosedException() {
    return new IOException("Socket is closed");
  }

  private IOException socketDisconnectedException() {
    return new IOException("Socket is disconnected");
  }

  private IOException initException() {
    if (Thread.currentThread().isInterrupted()) {
      return new InterruptedIOException("Operation interrupted");
    } else {
      return new ConnectException("Cannot connect socket");
    }
  }
}
