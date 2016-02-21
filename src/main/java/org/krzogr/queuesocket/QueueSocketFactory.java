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

import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketImpl;
import java.net.SocketImplFactory;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * SocketImplFactory which creates queue sockets.
 * 
 * This factory should be initialized globally before any sockets are created: {@code Socket.setSocketImplFactory()},
 * {@code ServerSocket.setSocketFactory()}.
 */
public class QueueSocketFactory implements SocketImplFactory {
  /**
   * Contains TRUE if the factory has already been initialized.
   */
  private static AtomicBoolean factorySet = new AtomicBoolean();

  @Override
  public SocketImpl createSocketImpl() {
    return new QueueSocket();
  }

  /**
   * Initializes the factory globally in the current process.
   * 
   * @throws RuntimeException if the factory has already been initialized.
   */
  public static void setup() {
    if (factorySet.compareAndSet(false, true)) {
      QueueSocketUtils.runAndRethrowRuntimeExceptionOnFailure(() -> {
        QueueSocketFactory newFactory = new QueueSocketFactory();

        Socket.setSocketImplFactory(newFactory);
        ServerSocket.setSocketFactory(newFactory);

        return null;
      }, "Cannot configure runtime to use " + QueueSocketFactory.class.getSimpleName());
    }
  }
}
