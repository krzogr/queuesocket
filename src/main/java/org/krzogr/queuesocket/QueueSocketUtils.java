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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Contains utility methods used by queue sockets.
 */
final class QueueSocketUtils {
  private QueueSocketUtils() {
    // Empty
  }

  /**
   * Attempts to sleep for the specified time without throwing InterruptedException.
   * 
   * Thread will still be marked as interrupted if sleep was interrupted.
   * 
   * @param time Time to sleep.
   * @param unit Time unit.
   */
  static void delayQuietly(final long time, final TimeUnit unit) {
    try {
      Thread.sleep(unit.toMillis(time));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Closes the specified closeable and ignores any exception thrown.
   * 
   * @param closeable Object to close.
   */
  static void closeQuietly(final Closeable closeable) {
    try {
      if (closeable != null) {
        closeable.close();
      }
    } catch (IOException e) {
      // Ignore the exception
    }
  }

  /**
   * Returns the bytes with "UTF-8" encoding from the specified string.
   * 
   * @param string String to retrieve bytes from.
   * @return Bytes retrieved from the string.
   * @throws RuntimeException if any error occurred.
   */
  static byte[] getBytes(final String string) {
    return getBytes(string, "UTF-8");
  }

  /**
   * Returns the bytes with the specified encoding from the specified string.
   * 
   * @param string String to retrieve bytes from.
   * @param encoding Encoding to use.
   * @return Bytes retrieved from the string.
   * @throws RuntimeException if any error occurred.
   */
  static byte[] getBytes(final String string, final String encoding) {
    return runAndRethrowRuntimeExceptionOnFailure(() -> {
      return string.getBytes(encoding);
    }, "Cannot extract bytes from string");
  }

  /**
   * Converts the specified bytes to string using "UTF-8" encoding.
   * 
   * @param bytes Bytes to convert.
   * @return String created from the given bytes.
   * @throws RuntimeException if any error occurred.
   */
  static String getString(byte[] bytes) {
    return getString(bytes, "UTF-8");
  }

  /**
   * Converts the specified bytes to string using the specified encoding.
   * 
   * @param bytes Bytes to convert.
   * @param encoding Encoding to use.
   * @return String created from the given bytes.
   * @throws RuntimeException if any error occurred.
   */
  static String getString(final byte[] bytes, final String encoding) {
    return runAndRethrowRuntimeExceptionOnFailure(() -> {
      return new String(bytes, encoding);
    }, "Cannot extract string from bytes");
  }

  /**
   * Runs the specified operation and re-throws any checked exception as RuntimeException.
   * 
   * @param operation Operation to run.
   * @param exceptionMessage Message to use when creating RuntimeException.
   * @return The result from the given operation.
   * @throws RuntimeException if any error occurred.
   */
  static <T> T runAndRethrowRuntimeExceptionOnFailure(final Callable<T> operation, final String exceptionMessage) {
    try {
      return operation.call();
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      } else {
        throw new RuntimeException(exceptionMessage, e);
      }
    }
  }
}
