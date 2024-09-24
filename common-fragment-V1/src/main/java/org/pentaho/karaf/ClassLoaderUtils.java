/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
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
