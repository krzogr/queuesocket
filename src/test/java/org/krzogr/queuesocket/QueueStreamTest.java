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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.krzogr.queuesocket.QueueSocketUtils.delayQuietly;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.krzogr.queuesocket.QueueStream;

/**
 * Contains unit tests for {@link QueueStream} class.
 */
public class QueueStreamTest extends AbstractQueueSocketTest {
  @Test
  public void testReadWriteSuccessfull() throws IOException {
    QueueStream stream = new QueueStream();

    stream.getOutputStream().write(new byte[] { 11, 12, 13 });

    int available = stream.getInputStream().available();
    assertEquals(3, available);

    byte[] buffer = new byte[3];
    int read = stream.getInputStream().read(buffer);

    assertEquals(3, read);
    assertArrayEquals(new byte[] { 11, 12, 13 }, buffer);
  }

  @Test
  public void readSingleByteIsSuccessfull() throws IOException {
    QueueStream stream = new QueueStream();
    stream.sendBytes(new byte[] { 7, 2, 3 });

    int result = stream.getInputStream().read();
    assertEquals(7, result);
  }

  @Test
  public void writeSingleByteSuccessfull() throws IOException {
    QueueStream stream = new QueueStream();
    stream.getOutputStream().write(9);
    stream.getOutputStream().write(8);
    stream.getOutputStream().write(7);

    int available = stream.getInputStream().available();
    assertEquals(3, available);
  }

  @Test
  public void readBufferFailsIfStreamIsClosed() throws Exception {
    final QueueStream stream = new QueueStream();
    final CountDownLatch aboutToReadFromStream = new CountDownLatch(1);

    Future<Integer> result = runInThread(() -> {
      byte[] data = new byte[10];

      aboutToReadFromStream.countDown();

      return stream.getInputStream().read(data);
    });

    aboutToReadFromStream.await();

    delayQuietly(50, TimeUnit.MILLISECONDS);

    stream.close();

    assertResult(result, -1);
  }

  @Test
  public void readSingleByteFailsIfStreamIsClosed() throws Exception {
    final QueueStream stream = new QueueStream();
    final CountDownLatch aboutToReadFromStream = new CountDownLatch(1);

    Future<Integer> result = runInThread(() -> {
      aboutToReadFromStream.countDown();

      return stream.getInputStream().read();
    });

    aboutToReadFromStream.await();

    delayQuietly(50, TimeUnit.MILLISECONDS);

    stream.close();

    assertResult(result, -1);
  }

  @Test
  public void readBufferFailsIfThreadIsInterrupted() throws Exception {
    final QueueStream stream = new QueueStream();

    final AtomicReference<Thread> threadRef = new AtomicReference<Thread>();
    final CountDownLatch aboutToReadFromStream = new CountDownLatch(1);

    Future<Object> result = runInThread(() -> {
      threadRef.set(Thread.currentThread());
      byte[] data = new byte[10];

      aboutToReadFromStream.countDown();

      return stream.getInputStream().read(data);
    });

    aboutToReadFromStream.await();
    threadRef.get().interrupt();

    assertFailure(result, InterruptedIOException.class, null);
  }

  @Test
  public void getNextBufferFailsIfThreadIsInterrupted() throws Exception {
    final QueueStream stream = new QueueStream();
    final AtomicReference<Thread> threadRef = new AtomicReference<Thread>();
    final CountDownLatch aboutToReadFromStream = new CountDownLatch(1);

    Future<Object> result = runInThread(() -> {
      threadRef.set(Thread.currentThread());

      aboutToReadFromStream.countDown();

      return stream.getNextBuffer(4, TimeUnit.SECONDS);
    });

    aboutToReadFromStream.await();
    threadRef.get().interrupt();

    assertFailure(result, RuntimeException.class, "Operation interrupted");
  }

  @Test
  public void numberOfAvailableBytesReturned() throws IOException {
    QueueStream stream = new QueueStream();
    stream.sendBytes(new byte[] { 7, 2, 3 });
    stream.sendBytes(new byte[] { 7 });
    stream.sendBytes(new byte[] { 9, 5 });

    int available = stream.getInputStream().available();
    assertEquals(6, available);

    int result = stream.getInputStream().read();
    assertEquals(7, result);

    available = stream.getInputStream().available();
    assertEquals(5, available);

    result = stream.getInputStream().read();
    assertEquals(2, result);

    available = stream.getInputStream().available();
    assertEquals(4, available);
  }
}
