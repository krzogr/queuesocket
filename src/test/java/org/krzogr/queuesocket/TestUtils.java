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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.junit.Assert;

/** Contains utility methods used in unit tests. */
public class TestUtils {
  /** Verifies that utility class is properly defined (private constructor, etc).
   * 
   * @param classObj Utility class to verify. */
  public static void assertUtilityClassWellDefined(final Class<?> classObj) {
    try {
      Assert.assertTrue("Class must be final", Modifier.isFinal(classObj.getModifiers()));
      Assert.assertEquals("There must be only one constructor", 1, classObj.getDeclaredConstructors().length);

      final Constructor<?> c = classObj.getDeclaredConstructor();

      if (c.isAccessible() || !Modifier.isPrivate(c.getModifiers())) {
        Assert.fail("Constructor is not private");
      }

      c.setAccessible(true);
      c.newInstance();
      c.setAccessible(false);

      for (final Method method : classObj.getMethods()) {
        if (!Modifier.isStatic(method.getModifiers()) && method.getDeclaringClass().equals(classObj)) {
          Assert.fail("There exists a non-static method:" + method);
        }
      }
    } catch (Exception e) {
      Assert.fail("Error while analysing utility class: " + e);
    }
  }
}
