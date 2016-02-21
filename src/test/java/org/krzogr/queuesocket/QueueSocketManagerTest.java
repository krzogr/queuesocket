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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.krzogr.queuesocket.QueueSocketEndpoint;
import org.krzogr.queuesocket.QueueStream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Contains unit tests for {@link QueueSocketManager} class.
 */
public class QueueSocketManagerTest extends AbstractQueueSocketTest {
  @Test
  public void exchangeClientSocketStreamReturnsNullIfInterrupted() throws Exception {
    final CountDownLatch aboutToExchangeStream = new CountDownLatch(1);
    final AtomicReference<Thread> childThread = new AtomicReference<Thread>();
    
    Future<QueueStream> result = runInThread(() -> {
        childThread.set(Thread.currentThread());
        aboutToExchangeStream.countDown();
        
        return manager.exchangeClientSocketStream("localhost", 10, new QueueStream(), 1, SECONDS);
      });

    aboutToExchangeStream.await();
    childThread.get().interrupt();
    
    assertResult(result, null);
  }

  @Test
  public void exchangeClientSocketStreamReturnsNullIfTimeoutExpired() throws Exception {
    Future<QueueStream> result = runInThread(() -> {
        return manager.exchangeClientSocketStream("localhost", 10, new QueueStream(), 1, MILLISECONDS);
      });

    assertResult(result, null);
  }
  
  @Test
  public void exchangeServerSocketStreamReturnsNullIfInterruped() throws Exception {
    final CountDownLatch aboutToExchangeStream = new CountDownLatch(1);
    final AtomicReference<Thread> childThread = new AtomicReference<Thread>();
    
    Future<QueueStream> result = runInThread(() -> {
        childThread.set(Thread.currentThread());
        aboutToExchangeStream.countDown();
        
        return manager.exchangeServerSocketStream("localhost", 10, new QueueStream(), 1, SECONDS);
      });

    aboutToExchangeStream.await();
    childThread.get().interrupt();
    
    assertResult(result, null);
  }

  @Test
  public void exchangeServerSocketStreamReturnsNullIfTimeoutExpired() throws Exception {
    Future<QueueStream> result = runInThread(() -> {
        return manager.exchangeServerSocketStream("localhost", 10, new QueueStream(), 1, MILLISECONDS);
      });

    assertResult(result, null);
  }
  
  @Test
  public void connectEndpointReturnsNullIfInterrupted() throws Exception {
    final CountDownLatch aboutToConnect = new CountDownLatch(1);
    final AtomicReference<Thread> childThread = new AtomicReference<Thread>();
    
    Future<QueueSocketEndpoint> result = runInThread(() -> {
        childThread.set(Thread.currentThread());
        aboutToConnect.countDown();
        
        return manager.connect("localhost", 10, 1, SECONDS);
      });

    aboutToConnect.await();
    childThread.get().interrupt();
    
    assertResult(result, null);
  }

  @Test
  public void connectEndpointReturnsNullTimeoutExpired() throws Exception {
    Future<QueueSocketEndpoint> result = runInThread(() -> {
        return manager.connect("localhost", 10, 1, MILLISECONDS);
      });

    assertResult(result, null);
  }
  
  @Test
  public void acceptEndpointReturnsNullIfInterruped() throws Exception {
    final CountDownLatch aboutToAccept = new CountDownLatch(1);
    final AtomicReference<Thread> childThread = new AtomicReference<Thread>();
    
    Future<QueueSocketEndpoint> result = runInThread(() -> {
        childThread.set(Thread.currentThread());
        aboutToAccept.countDown();
        
        return manager.accept("localhost", 10, 10, SECONDS);
      });

    aboutToAccept.await();
    childThread.get().interrupt();
    
    assertResult(result, null);
  }

  @Test
  public void acceptEndpointReturnsNullIfTimeoutExpired() throws Exception {
    Future<QueueSocketEndpoint> result = runInThread(() -> {
        return manager.accept("localhost", 10, 1, MILLISECONDS);
      });

    assertResult(result, null);
  }
}
