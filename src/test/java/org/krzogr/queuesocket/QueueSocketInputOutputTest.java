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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.krzogr.queuesocket.QueueSocketUtils.getString;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Contains unit tests which verify input/output scenarios for {@link QueueSocket} class.
 */
public class QueueSocketInputOutputTest extends AbstractQueueSocketTest {
  @Test
  public void bytesReadFromSocketAsString() throws Exception {
    runInThread(() -> {
      Socket socket = new Socket();
      socket.connect(new InetSocketAddress("localhost", 2030));

      byte[] data = new byte[10];
      int read = socket.getInputStream().read(data);

      BufferedOutputStream output = new BufferedOutputStream(socket.getOutputStream());

      if (read > 0) {
        output.write("response for: ".getBytes());
        output.write(data, 0, read);
      } else {
        output.write("error: expected to read something first".getBytes());
      }

      output.flush();
      socket.close();
      return null;
    });

    QueueSocketEndpoint endpoint = manager.accept("localhost", 2030, 1, SECONDS);
    endpoint.sendMessage("req_123");

    String response = endpoint.getNextReceivedString(1, SECONDS);
    assertEquals("response for: req_123", response);
  }

  @Test
  public void bytesReadFromSocketAndEndpointWithChunks() throws Exception {
    runInThread(() -> {
      Socket socket = new Socket();
      socket.connect(new InetSocketAddress("localhost", 2030));

      byte[] data1 = new byte[2];
      byte[] data2 = new byte[2];
      byte[] data3 = new byte[10];

      int read1 = socket.getInputStream().read(data1, 0, 2);
      int read2 = socket.getInputStream().read(data2, 0, 2);
      int read3 = socket.getInputStream().read(data3, 0, 10);

      BufferedOutputStream output = new BufferedOutputStream(socket.getOutputStream());

      if (read1 > 0 && read2 > 0 && read3 > 0) {
        output.write("response for: ".getBytes());
        output.write(data1, 0, read1);
        output.write(data2, 0, read2);
        output.write(data3, 0, read3);
      } else {
        output.write("error: expected to read something first".getBytes());
      }

      output.flush();
      socket.close();
      return null;
    });

    QueueSocketEndpoint endpoint = manager.accept("localhost", 2030, 1, SECONDS);
    endpoint.sendMessage("req_123");

    byte[] response = endpoint.getReceivedBytes(2, 1, SECONDS);
    assertEquals("re", getString(response));

    response = endpoint.getReceivedBytes(2, 1, SECONDS);
    assertEquals("sp", getString(response));

    response = endpoint.getReceivedBytes(4, 1, SECONDS);
    assertEquals("onse", getString(response));

    response = endpoint.getAvailableBytes(15, 1, SECONDS);
    assertEquals(" for: req_123", getString(response));
  }

  @Test
  public void bytesWrittenToEndpointWithChunks() throws Exception {
    runInThread(() -> {
      Socket socket = new Socket();
      socket.connect(new InetSocketAddress("localhost", 2030));

      byte[] data1 = new byte[4];
      byte[] data2 = new byte[4];

      int read1 = socket.getInputStream().read(data1, 0, 4);
      int read2 = socket.getInputStream().read(data2, 0, 4);

      BufferedOutputStream output = new BufferedOutputStream(socket.getOutputStream());

      if (read1 > 0 && read2 > 0) {
        output.write("response for: ".getBytes());
        output.write(data1, 0, read1);
        output.write(data2, 0, read2);
      } else {
        output.write("error: expected to read something first".getBytes());
      }

      output.flush();
      socket.close();
      return null;
    });

    QueueSocketEndpoint endpoint = manager.accept("localhost", 2030, 1, SECONDS);
    endpoint.sendMessage("1234");
    endpoint.sendMessage("5678");

    byte[] response = endpoint.getAvailableBytes(22, 1, SECONDS);
    assertEquals("response for: 12345678", QueueSocketUtils.getString(response, "UTF-8"));
  }

  @Test
  public void exactReadFromEndpointFailsIfTimeoutExpired() throws Exception {
    runInThread(() -> {
      Socket socket = new Socket();
      socket.connect(new InetSocketAddress("localhost", 2030));

      byte[] data = new byte[10];
      int read = socket.getInputStream().read(data);

      BufferedOutputStream output = new BufferedOutputStream(socket.getOutputStream());

      if (read > 0) {
        output.write("response for: ".getBytes());
        output.write(data, 0, read);
      } else {
        output.write("error: expected to read something first".getBytes());
      }

      output.flush();
      socket.close();
      return null;
    });

    QueueSocketEndpoint endpoint = manager.accept("localhost", 2030, 1, SECONDS);
    endpoint.sendMessage("req_123");

    byte[] bytes = endpoint.getReceivedBytes(30, 1, SECONDS);
    assertNull(bytes);
  }

  @Test
  public void stringReadFromEndpointFailsIfTimeoutExpired() throws Exception {
    runInThread(() -> {
      Socket socket = new Socket();
      socket.connect(new InetSocketAddress("localhost", 2030));

      socket.getInputStream().read();
      return null;
    });

    QueueSocketEndpoint endpoint = manager.accept("localhost", 2030, 1, SECONDS);
    endpoint.getNextReceivedString(1, SECONDS);
  }

  @Test
  public void exactReadFromEndpointReturnsNullIfInterrupted() throws Exception {
    runInThread(() -> {
      try {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress("localhost", 2030));

        byte[] data = new byte[10];
        int read = socket.getInputStream().read(data);

        BufferedOutputStream output = new BufferedOutputStream(socket.getOutputStream());

        if (read > 0) {
          output.write("response for: ".getBytes());
          output.write(data, 0, read);
        } else {
          output.write("error: expected to read something first".getBytes());
        }

        output.flush();
        socket.close();
        return null;
      } catch (Exception e) {
        e.printStackTrace();
        return null;
      }
    });

    final CountDownLatch aboutToReadFromEndpoint = new CountDownLatch(1);
    final AtomicReference<Thread> thread = new AtomicReference<Thread>();

    Future<byte[]> result = runInThread(() -> {
      thread.set(Thread.currentThread());
      QueueSocketEndpoint endpoint = manager.accept("localhost", 2030, 1, SECONDS);

      endpoint.sendMessage("req_123");
      aboutToReadFromEndpoint.countDown();

      return endpoint.getReceivedBytes(30, 1, MILLISECONDS);
    });

    aboutToReadFromEndpoint.await();
    thread.get().interrupt();

    assertResult(result, null);
  }

  @Test
  public void nonExactReadFromEndpointReturnsAvailableBytesWhenInterrupted() throws Exception {
    final CountDownLatch dataWrittenToSocket = new CountDownLatch(1);

    runInThread(() -> {
      Socket socket = new Socket();
      socket.connect(new InetSocketAddress("localhost", 2030));

      byte[] data = new byte[10];
      int read = socket.getInputStream().read(data);

      BufferedOutputStream output = new BufferedOutputStream(socket.getOutputStream());

      if (read > 0) {
        output.write("response for: ".getBytes());
        output.write(data, 0, read);
      } else {
        output.write("error: expected to read something first".getBytes());
      }

      output.flush();
      dataWrittenToSocket.countDown();

      socket.close();
      return null;
    });

    final CountDownLatch aboutToReadFromEndpoint = new CountDownLatch(1);
    final AtomicReference<Thread> thread = new AtomicReference<Thread>();

    Future<byte[]> result = runInThread(() -> {
      thread.set(Thread.currentThread());
      QueueSocketEndpoint endpoint = manager.accept("localhost", 2030, 1, SECONDS);

      endpoint.sendMessage("req_123");
      aboutToReadFromEndpoint.countDown();

      return endpoint.getAvailableBytes(30, 1, MILLISECONDS);
    });

    dataWrittenToSocket.await();
    aboutToReadFromEndpoint.await();

    thread.get().interrupt();

    assertNotNull(result.get());
    assertTrue(result.get().length > 0);
  }

  @Test
  public void nonExactReadFromEndpointReturnsNullWhenTimeoutExpired() throws Exception {
    runInThread(() -> {
      Socket socket = new Socket();
      socket.connect(new InetSocketAddress("localhost", 2030));
      return null;
    });

    final CountDownLatch aboutToReadFromEndpoint = new CountDownLatch(1);
    final AtomicReference<Thread> thread = new AtomicReference<Thread>();

    Future<byte[]> result = runInThread(() -> {
      thread.set(Thread.currentThread());
      QueueSocketEndpoint endpoint = manager.accept("localhost", 2030, 1, SECONDS);

      aboutToReadFromEndpoint.countDown();

      return endpoint.getAvailableBytes(30, 1, MILLISECONDS);
    });

    aboutToReadFromEndpoint.await();

    thread.get().interrupt();

    assertResult(result, null);
  }

  @Test
  public void readFromSocketFailsIfSocketIsClosed() {
    Future<String> result = runInThread(() -> {
      Socket socket = new Socket();
      socket.connect(new InetSocketAddress("localhost", 2030));

      InputStream input = socket.getInputStream();

      socket.close();

      byte[] data = new byte[10];
      input.read(data);

      return "data read";
    });

    manager.accept("localhost", 2030, 1, SECONDS);
    assertFailure(result, IOException.class, "Stream is closed");
  }

  @Test
  public void writeToSocketFailsIfSocketIsClosed() {
    Future<String> result = runInThread(() -> {
      Socket socket = new Socket();
      socket.connect(new InetSocketAddress("localhost", 2030));

      OutputStream output = socket.getOutputStream();

      socket.close();

      output.write("test".getBytes());

      return "data written";
    });

    manager.accept("localhost", 2030, 1, SECONDS);
    assertFailure(result, IOException.class, "Stream is closed");
  }

  @Test
  public void bytesReadAndWrittenAfterServerSocketIsAccepted() throws Exception {
    runInThread(() -> {
      ServerSocket serverSocket = new ServerSocket(2032);
      Socket socket = serverSocket.accept();

      byte[] data = new byte[10];
      int read = socket.getInputStream().read(data);

      BufferedOutputStream output = new BufferedOutputStream(socket.getOutputStream());

      if (read > 0) {
        output.write("response for: ".getBytes());
        output.write(data, 0, read);
      } else {
        output.write("error: expected to read something first".getBytes());
      }

      output.flush();
      socket.close();
      return null;
    });

    QueueSocketEndpoint endpoint = manager.connect("localhost", 2032, 1, SECONDS);
    endpoint.sendMessage(QueueSocketUtils.getBytes("req_123", "UTF-8"));

    String response = endpoint.getNextReceivedString(1, SECONDS);

    assertEquals("response for: req_123", response);
  }

  @Test
  public void socketInputStreamAvailableBytesReturned() throws Exception {
    Future<Integer> result = runInThread(() -> {
      Socket socket = new Socket();
      socket.connect(new InetSocketAddress("localhost", 2030));

      InputStream input = socket.getInputStream();

      long start = System.currentTimeMillis();

      while (input.available() <= 0 && (System.currentTimeMillis() - start) < 500) {
        Thread.sleep(10);
      }

      return input.available();
    });

    QueueSocketEndpoint endpoint = manager.accept("localhost", 2030, 1, SECONDS);
    endpoint.sendMessage("req_123");

    assertEquals(7, result.get().intValue());
  }

  @Test
  public void socketAvailableBytesReturned() throws Exception {
    QueueStream input = new QueueStream();
    input.sendBytes("abc");

    QueueSocket impl = new QueueSocket(input, new QueueStream());
    int available = impl.available();
    assertEquals(3, available);
  }

  @Test
  public void socketOptionGetAndSet() throws Exception {
    Future<Boolean> result = runInThread(() -> {
      ServerSocket serverSocket = new ServerSocket();
      serverSocket.bind(new InetSocketAddress("0.0.0.0", 0));

      Socket socket = serverSocket.accept();

      int value = socket.getSendBufferSize();
      socket.setSendBufferSize(value + 10);
      return true;
    });

    QueueSocketEndpoint endpoint = manager.connect("localhost", 0, 1, SECONDS);
    assertNotNull(endpoint);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void socketUrgentDataUnsupported() throws Exception {
    QueueSocket impl = new QueueSocket();
    impl.sendUrgentData(1);
  }

  @Test
  public void socketGetInputStreamFailsIfSocketIsClosed() throws Exception {
    Future<Object> result = runInThread(() -> {
      QueueSocket impl = new QueueSocket();
      impl.connect("localhost", 2030);
      impl.close();
      impl.getInputStream();
      return true;
    });

    manager.accept("localhost", 2030, 1, SECONDS);

    assertFailure(result, IOException.class, "Socket is closed");
  }

  @Test
  public void socketGetOutputStreamFailsIfSocketIsClosed() throws Exception {
    Future<Object> result = runInThread(() -> {
      QueueSocket impl = new QueueSocket();
      impl.connect("localhost", 2030);
      impl.close();
      impl.getOutputStream();
      return true;
    });

    manager.accept("localhost", 2030, 1, SECONDS);

    assertFailure(result, IOException.class, "Socket is closed");
  }

  @Test(expected = IOException.class)
  public void socketImplGetInputStreamFailsIfSocketIsDisconnected() throws Exception {
    QueueSocket impl = new QueueSocket();
    impl.getInputStream();
  }

  @Test(expected = IOException.class)
  public void socketImplGetOutputStreamFailsIfSocketIsDisconnected() throws Exception {
    QueueSocket impl = new QueueSocket();
    impl.getOutputStream();
  }
}
