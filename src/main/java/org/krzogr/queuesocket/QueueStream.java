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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Stream associated with an in-memory queue.
 * 
 * This class enables to read from the queue with InputStream and write to the queue with OutputStream. This class
 * supports different threads to read and write to the queue using corresponding streams. However using a single stream
 * from multiple threads is not supported (i.e. reading from the queue by using InputStream from two different threads).
 */
public class QueueStream {
  private static final int UNSIGNED_BYTE_MASK = 0x000000FF;

  /**
   * Special array enqueued to indicate that stream is closed.
   */
  private static final byte[] EMPTY = new byte[] {};

  /**
   * Contains TRUE if stream is closed.
   */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Queue containing the bytes associated with the stream.
   */
  private final LinkedBlockingDeque<byte[]> queue = new LinkedBlockingDeque<byte[]>();

  /**
   * Input stream associated with the queue.
   */
  private final QueueInputStream inputStream = new QueueInputStream();

  /**
   * Output stream associated with the queue.
   */
  private final QueueOutputStream outputStream = new QueueOutputStream();

  /**
   * Input stream which enables to read from the queue.
   */
  private class QueueInputStream extends InputStream {
    private ByteBuffer readBuffer;

    @Override
    public int read() throws IOException {
      verifyStreamIsOpen();
      ByteBuffer buffer = getReadBuffer();

      if (buffer.hasRemaining()) {
        byte result = buffer.get();
        return result & UNSIGNED_BYTE_MASK;
      } else {
        return -1;
      }
    }

    @Override
    public int read(final byte[] b) throws IOException {
      return read(b, 0, b.length);
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
      verifyStreamIsOpen();
      ByteBuffer buffer = getReadBuffer();

      if (buffer.hasRemaining()) {
        int toCopy = Math.min(buffer.remaining(), Math.min(len, b.length - off));
        buffer.get(b, off, toCopy);
        return toCopy;
      } else {
        return -1;
      }
    }

    @Override
    public int available() throws IOException {
      verifyStreamIsOpen();

      int result = 0;

      if (readBuffer != null) {
        result += readBuffer.remaining();
      }

      for (byte[] data : queue) {
        result += data.length;
      }

      return result;
    }

    private ByteBuffer getReadBuffer() throws IOException {
      verifyStreamIsOpen();

      if (readBuffer != null && readBuffer.hasRemaining()) {
        return readBuffer;
      } else {
        while (true) {
          try {
            byte[] buffer = queue.take();
            readBuffer = ByteBuffer.wrap(buffer);
            return readBuffer;
          } catch (InterruptedException e) {
            throw new InterruptedIOException();
          }
        }
      }
    }

    @Override
    public void close() {
      closed.set(true);
      queue.add(EMPTY);
    }
  }

  /**
   * Output stream which enables to write to the queue.
   */
  private class QueueOutputStream extends OutputStream {
    @Override
    public void write(final int b) throws IOException {
      verifyStreamIsOpen();
      queue.add(new byte[] { (byte) b });
    }

    @Override
    public void write(final byte[] b) throws IOException {
      verifyStreamIsOpen();
      queue.add(b);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
      verifyStreamIsOpen();
      queue.add(Arrays.copyOfRange(b, off, off + len));
    }

    @Override
    public void close() {
      closed.set(true);
    }
  }

  public InputStream getInputStream() {
    return inputStream;
  }

  public OutputStream getOutputStream() {
    return outputStream;
  }

  private void verifyStreamIsOpen() throws IOException {
    if (closed.get()) {
      throw new IOException("Stream is closed");
    }
  }

  /**
   * Returns the next byte buffer from the queue.
   * 
   * @param time Maximum time to wait for the data.
   * @param unit Time unit.
   * @return Next byte buffer retrieved from the queue or NULL if the specified time expired.
   */
  public byte[] getNextBuffer(final long time, final TimeUnit unit) {
    try {
      return queue.poll(time, unit);
    } catch (InterruptedException e) {
      throw new RuntimeException("Operation interrupted");
    }
  }

  /**
   * Get the specified number of bytes from the queue.
   * 
   * @param count Number of bytes to return.
   * @param exact True if the number of bytes should be exactly matched. False if any number of bytes retrieved uop to
   *          this number can be returned.
   * @param time Maximum time to wait for the data.
   * @param unit Time unit.
   * @return The specified number of bytes retrieved from the queue or NULL if the time expired.
   */
  public byte[] getNextBytes(final int count, final boolean exact, final long time, final TimeUnit unit) {
    ByteBuffer result = ByteBuffer.allocate(count);
    long startTimeMillis = System.currentTimeMillis();
    long maxTimeMillis = unit.toMillis(time);
    int copied = 0;

    try {
      while (true) {
        long elapsedTimeMillis = System.currentTimeMillis() - startTimeMillis;

        if (elapsedTimeMillis >= maxTimeMillis) {
          return extractBytes(count, exact, result, copied);
        } else {
          byte[] bytes = queue.poll(maxTimeMillis - elapsedTimeMillis, TimeUnit.MILLISECONDS);
          copied += addBytes(result, count - copied, bytes);

          if (copied == count) {
            return result.array();
          }
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return extractBytes(count, exact, result, copied);
    }
  }

  private byte[] extractBytes(final int count, final boolean exact, final ByteBuffer result, final int copied) {
    byte[] bytes = null;

    if (exact) {
      if (copied > 0) {
        // Return the retrieved number of bytes back to the queue
        queue.addFirst(Arrays.copyOfRange(result.array(), 0, copied));
      }
    } else {
      // If exact number of bytes was not requested then return whatever was read
      if (copied > 0) {
        bytes = Arrays.copyOfRange(result.array(), 0, copied);
      }
    }

    return bytes;
  }

  private int addBytes(final ByteBuffer result, final int needed, final byte[] bytes) {
    if (bytes != null) {
      int toCopy = Math.min(bytes.length, needed);
      result.put(bytes, 0, toCopy);

      if (toCopy < bytes.length) {
        // Add the remaining part of the current buffer back to the queue
        queue.addFirst(Arrays.copyOfRange(bytes, toCopy, bytes.length));
      }

      return toCopy;
    } else {
      return 0;
    }
  }

  /**
   * Adds the bytes of the specified string to the queue so that they can be read by the socket.
   * 
   * @param string String whose bytes should be added to the queue.
   */
  public void sendBytes(final String string) {
    Objects.requireNonNull(string);
    queue.add(QueueSocketUtils.getBytes(string));
  }

  /**
   * Adds the specified bytes to the queue so that they can be read by the socket.
   * 
   * @param bytes Bytes to add.
   */
  public void sendBytes(final byte[] bytes) {
    Objects.requireNonNull(bytes);
    queue.add(bytes);
  }

  public void close() {
    inputStream.close();
    outputStream.close();
  }
}
