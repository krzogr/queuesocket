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
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.krzogr.queuesocket.QueueSocketUtils;
import org.krzogr.queuesocket.TestUtils;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Contains unit tests for {@link QueueSocketUtils} class.
 */
public class QueueSocketUtilsTest extends AbstractQueueSocketTest {
  @Test(expected = RuntimeException.class)
  public void getBytesWithInvalidEncodingFails() {
    QueueSocketUtils.getBytes("abc", "invalid");
  }

  @Test(expected = RuntimeException.class)
  public void getStringWithInvalidEncodingFails() {
    QueueSocketUtils.getString(new byte[] { 64, 64, 64 }, "invalid");
  }

  @Test
  public void objectClosedWithoutExceptionThrown() {
    QueueSocketUtils.closeQuietly(new Closeable() {
      @Override
      public void close() throws IOException {
        throw new IOException();
      }
    });
  }

  @Test
  public void delayWithoutInterrruptedExceptionThrown() throws Exception {
    final CountDownLatch aboutToDelay = new CountDownLatch(1);
    final AtomicReference<Thread> childThread = new AtomicReference<Thread>();

    Future<Object> result = runInThread(() -> {
      childThread.set(Thread.currentThread());
      aboutToDelay.countDown();
      
      QueueSocketUtils.delayQuietly(5, SECONDS);
      
      return "OK";
    });

    aboutToDelay.await();
    childThread.get().interrupt();

    assertResult(result, "OK");
  }

  @Test
  public void queueSocketUtilsClassWellDefined() {
    TestUtils.assertUtilityClassWellDefined(QueueSocketUtils.class);
  }

  @Test
  public void originalRuntimeExceptionRethrown() {
    Throwable error = null;

    try {
      QueueSocketUtils.runAndRethrowRuntimeExceptionOnFailure(new Callable<String>() {
        @Override
        public String call() throws Exception {
          throw new IllegalArgumentException("Test");
        }
      }, "Runtime exception prefix");
    } catch (Throwable e) {
      error = e;
    }

    assertNotNull(error);
    assertTrue(error instanceof IllegalArgumentException);
    assertEquals("Test", error.getMessage());
  }
}
