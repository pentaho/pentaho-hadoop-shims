/*******************************************************************************
 * Pentaho Big Data
 * <p/>
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 * <p/>
 * ******************************************************************************
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package org.pentaho.karaf;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ClassLoaderUtils {

  private ClassLoaderUtils() { }

  public static Class findClass( ClassLoader classLoader, String className ) {
    Class classFound = null;
    Class classLoaderClass = classLoader.getClass();
    do {
      try {
        Method findClassMethod = classLoaderClass.getDeclaredMethod( "findClass", new Class[] { String.class } );
        findClassMethod.setAccessible( true );
        classFound = (Class) findClassMethod.invoke( classLoader, new Object[] { className } );
        if ( classFound != null ) {
          break;
        } else {
          classLoaderClass = classLoaderClass.getSuperclass();
        }
      } catch ( NoSuchMethodException | IllegalAccessException | InvocationTargetException e ) {
        classLoaderClass = classLoaderClass.getSuperclass();
      }
    } while ( classLoaderClass != null );
    return classFound;
  }
}
