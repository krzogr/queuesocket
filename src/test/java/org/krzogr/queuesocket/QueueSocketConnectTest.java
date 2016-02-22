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
import static org.krzogr.queuesocket.QueueSocketUtils.delayQuietly;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.krzogr.queuesocket.QueueSocketEndpoint;
import org.krzogr.queuesocket.QueueSocket;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Contains unit tests which verify connect/disconnect scenarios for {@link QueueSocket} class.
 */
public class QueueSocketConnectTest extends AbstractQueueSocketTest {
  @Test
  public void socketConnectSuccessfull() throws Exception {
    Future<String> result = runInThread(() -> {
      Socket socket = new Socket();
      socket.connect(new InetSocketAddress("localhost", 2030));
      socket.close();
      return "connected";
    });

    manager.accept("localhost", 2030, 1, SECONDS);

    assertResult(result, "connected");
  }

  @Test
  public void socketConnectWithTimeLimitSuccessfull() throws Exception {
    Future<String> result = runInThread(() -> {
      Socket socket = new Socket();
      socket.connect(new InetSocketAddress("localhost", 2030), 100);
      return "connected";
    });

    manager.accept("localhost", 2030, 1, SECONDS);

    assertResult(result, "connected");
  }

  @Test
  public void socketImplConnectByInetAddressSuccessfull() throws Exception {
    Future<String> result = runInThread(() -> {
      QueueSocket impl = new QueueSocket();
      impl.connect(InetAddress.getByName("localhost"), 2030);
      return "connected";
    });

    manager.accept("localhost", 2030, 10, SECONDS);

    assertResult(result, "connected");
  }

  @Test
  public void socketImplConnectByHostPortSuccessfull() throws Exception {
    Future<String> result = runInThread(() -> {
      QueueSocket impl = new QueueSocket();
      impl.connect("localhost", 2030);
      return "connected";
    });

    manager.accept("localhost", 2030, 10, SECONDS);

    assertResult(result, "connected");
  }

  @Test
  public void serverSocketAcceptSuccessfull() throws Exception {
    Future<String> result = runInThread(() -> {
      ServerSocket socket = new ServerSocket(2032);
      Socket client = socket.accept();
      return "accepted";
    });

    QueueSocketEndpoint endpoint = manager.connect("localhost", 2032, 1, SECONDS);

    assertResult(result, "accepted");
    assertNotNull(endpoint);
    assertEquals("localhost", endpoint.getServer());
    assertEquals(2032, endpoint.getPort());
  }

  @Test
  public void serverSocketOnPort0AcceptSuccessfull() throws Exception {
    Future<String> result = runInThread(() -> {
      ServerSocket serverSocket = new ServerSocket();
      serverSocket.bind(new InetSocketAddress("0.0.0.0", 0));

      Socket socket = serverSocket.accept();
      return "accepted";
    });

    QueueSocketEndpoint endpoint = manager.connect("localhost", 0, 1, SECONDS);

    assertResult(result, "accepted");
    assertNotNull(endpoint);
    assertEquals("localhost", endpoint.getServer());
    assertEquals(0, endpoint.getPort());
  }

  @Test
  public void socketConnectByIpAddress1Successfull() throws Exception {
    runInThread(() -> {
      Socket socket = new Socket();
      socket.connect(new InetSocketAddress("127.0.0.1", 2030));
      return null;
    });

    QueueSocketEndpoint endpoint = manager.accept("localhost", 2030, 10, SECONDS);
    assertNotNull(endpoint);
  }

  @Test
  public void socketConnectByIpAddress2Successfull() throws Exception {
    runInThread(() -> {
      Socket socket = new Socket();
      socket.connect(new InetSocketAddress("0.0.0.0", 2030));
      return null;
    });

    QueueSocketEndpoint endpoint = manager.accept("localhost", 2030, 10, SECONDS);
    assertNotNull(endpoint);
  }

  @Test(expected = ConnectException.class)
  public void socketConnectFailsIfRemoteSideIsNotListenig() throws Exception {
    manager.setMaxConnectTimeMillis(100);

    Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", 2030));
  }

  @Test
  public void endpointConnectFailsIfRemoteSideIsNotListenig() throws Exception {
    QueueSocketEndpoint endpoint = manager.connect("localhost", 2030, 200, MILLISECONDS);
    assertNull(endpoint);
  }

  @Test
  public void endpointAcceptFailsIfRemoteSideIsNotConnecting() throws Exception {
    QueueSocketEndpoint endpoint = manager.accept("localhost", 2030, 200, MILLISECONDS);
    assertNull(endpoint);
  }

  @Test
  public void socketConnectFailsIfThreadIsInterrupted() throws Exception {
    final CountDownLatch socketAboutToConnect = new CountDownLatch(1);
    final AtomicReference<Thread> thread = new AtomicReference<Thread>();

    Future<Object> result = runInThread(() -> {
      thread.set(Thread.currentThread());
      socketAboutToConnect.countDown();

      Socket socket = new Socket();
      socket.connect(new InetSocketAddress("localhost", 2030));

      return true;
    });

    socketAboutToConnect.await();
    thread.get().interrupt();

    assertFailure(result, InterruptedIOException.class, "Operation interrupted");
  }

  @Test
  public void socketConnectFailsSocketIsClosedInTheMeantime() throws Exception {
    final CountDownLatch socketAboutToConnect = new CountDownLatch(1);
    final AtomicReference<Socket> socketRef = new AtomicReference<Socket>();

    Future<Object> result = runInThread(() -> {
      Socket socket = new Socket();

      socketRef.set(socket);
      socketAboutToConnect.countDown();

      socket.connect(new InetSocketAddress("localhost", 2030));
      return true;
    });

    socketAboutToConnect.await();

    delayQuietly(50, MILLISECONDS);

    socketRef.get().close();

    manager.accept("localhost", 2030, 1, SECONDS);

    assertFailure(result, IOException.class, "Socket is closed");
  }

  @Test
  public void socketConnectFailsIfEndpointConnectIsCalled() throws Exception {
    Future<Object> result = runInThread(() -> {
      Socket socket = new Socket();
      socket.connect(new InetSocketAddress("localhost", 2030), 500);
      return true;
    });

    QueueSocketEndpoint endpoint = manager.connect("localhost", 2030, 1, SECONDS);
    assertNull(endpoint);
    
    assertFailure(result, IOException.class, "Cannot connect socket");
  }

  @Test
  public void socketAcceptFailsIfEndpointAcceptIsCalled() throws Exception {
    AtomicReference<ServerSocket> socketRef = new AtomicReference<>();
    
    runInThread(() -> {
      ServerSocket socket = new ServerSocket(2030);
      socketRef.set(socket);
      socket.accept();
      return true;
    });

    QueueSocketEndpoint endpoint = manager.accept("localhost", 2030, 1, SECONDS);
    assertNull(endpoint);

    socketRef.get().close();
  }
  
  @Test
  public void socketImplConnectFailsIfSocketIsClosedInTheMeantime() throws Exception {
    final CountDownLatch socketAboutToConnect = new CountDownLatch(1);
    final AtomicReference<QueueSocket> socketRef = new AtomicReference<QueueSocket>();

    Future<Object> result = runInThread(() -> {
      QueueSocket socket = new QueueSocket();

      socketRef.set(socket);
      socketAboutToConnect.countDown();

      socket.connect("localhost", 2030);
      return true;
    });

    socketAboutToConnect.await();

    delayQuietly(50, MILLISECONDS);

    socketRef.get().close();

    manager.accept("localhost", 2030, 1, SECONDS);

    assertFailure(result, IOException.class, "Socket is closed");
  }
}
