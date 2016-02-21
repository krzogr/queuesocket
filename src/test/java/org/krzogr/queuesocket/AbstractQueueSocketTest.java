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
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.krzogr.queuesocket.QueueSocketFactory;
import org.krzogr.queuesocket.QueueSocketManager;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Contains common functionality used in queue socket unit tests.
 */
public abstract class AbstractQueueSocketTest {
  protected ExecutorService executor;

  protected QueueSocketManager manager;

  @Before
  public void prepare() {
    QueueSocketFactory.setup();
    executor = Executors.newFixedThreadPool(3);
    manager = QueueSocketManager.getInstance();
  }

  @After
  public void cleanup() {
    executor.shutdownNow();
    manager.reset();
  }

  protected <T> Future<T> runInThread(Callable<T> test) {
    return executor.submit(test);
  }

  protected void assertResult(Future<?> future, Object expected) {
    assertResult(future, expected, 1, SECONDS);
  }

  protected void assertResult(Future<?> future, Object expected, long time, TimeUnit unit) {
    try {
      Object actual = future.get(time, unit);
      assertEquals(expected, actual);
    } catch (TimeoutException e) {
      assertTrue("Cannot retrieve result within the given time", false);
    } catch (ExecutionException e) {
      assertTrue("Expected result but the operation failed: " + e, false);
    } catch (InterruptedException e) {
      assertTrue("Cannot retrieve result within the given time", false);
    }
  }

  protected void assertFailure(Future<?> future, Class<? extends Throwable> errorType, String errorMessage) {
    assertFailure(future, errorType, errorMessage, 1, SECONDS);
  }

  protected void assertFailure(Future<?> future, Class<? extends Throwable> errorType, String errorMessage, long time,
      TimeUnit unit) {
    try {
      Object actual = future.get(time, unit);
      assertTrue("Expected failure but the operation was successful: " + actual, false);
    } catch (TimeoutException e) {
      assertTrue("Cannot retrieve result within the given time", false);
    } catch (ExecutionException e) {
      Throwable error = e.getCause();
      assertTrue(errorType.isInstance(error));
      assertEquals(errorMessage, error.getMessage());
    } catch (InterruptedException e) {
      assertTrue("Cannot retrieve result within the given time", false);
    }
  }
}
