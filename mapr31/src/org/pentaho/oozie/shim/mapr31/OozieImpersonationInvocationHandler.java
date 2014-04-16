/*******************************************************************************
*
* Pentaho Big Data
*
* Copyright (C) 2002-2014 by Pentaho : http://www.pentaho.com
*
*******************************************************************************
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
******************************************************************************/

package org.pentaho.oozie.shim.mapr31;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.lang.ClassUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.oozie.client.OozieClient;

public class OozieImpersonationInvocationHandler<T> implements InvocationHandler {
  private final T delegate;
  private final Set<Class<?>> interfacesToDelegate;
  private final String user;

  public OozieImpersonationInvocationHandler( T delegate ) {
    this( delegate, new HashSet<Class<?>>(), null );
  }

  public OozieImpersonationInvocationHandler( T delegate, Set<Class<?>> interfacesToDelegate ) {
    this( delegate, interfacesToDelegate, null );
  }

  public OozieImpersonationInvocationHandler( T delegate, Set<Class<?>> interfacesToDelegate, String user ) {
    this.delegate = delegate;
    this.interfacesToDelegate = interfacesToDelegate;
    this.user = user;
  }

  public static <T> T forObject( T delegate, Set<Class<?>> interfacesToDelegate ) {
    return forObject( delegate, interfacesToDelegate, null );
  }

  @SuppressWarnings( "unchecked" )
  public static <T> T forObject( T delegate, Set<Class<?>> interfacesToDelegate, String user ) {
    return (T) Proxy.newProxyInstance( delegate.getClass().getClassLoader(), (Class<?>[]) ClassUtils.getAllInterfaces(
        delegate.getClass() ).toArray( new Class<?>[] {} ), new OozieImpersonationInvocationHandler<Object>( delegate,
        interfacesToDelegate, user ) );
  }

  private <RunType> RunType runAsUser( final PrivilegedExceptionAction<RunType> action, String user ) throws Throwable {
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader( OozieImpersonationInvocationHandler.class.getClassLoader() );
      return OozieClient.doAs( user, new Callable<RunType>() {

        @Override
        public RunType call() throws Exception {
          return action.run();
        }
      } );
    } catch ( Exception e ) {
      Throwable actualException = e;
      if ( actualException instanceof UndeclaredThrowableException ) {
        actualException = actualException.getCause();
      }
      if ( actualException instanceof PrivilegedActionException ) {
        actualException = actualException.getCause();
      }
      throw actualException;
    } finally {
      Thread.currentThread().setContextClassLoader( contextClassLoader );
    }
  }

  @Override
  public Object invoke( Object proxy, final Method method, final Object[] args ) throws Throwable {
    PrivilegedExceptionAction<Object> action = new PrivilegedExceptionAction<Object>() {

      @SuppressWarnings( "unchecked" )
      @Override
      public Object run() throws Exception {
        Object result = method.invoke( delegate, args );
        if ( result != null ) {
          for ( Class<?> iface : (Class<?>[]) ClassUtils.getAllInterfaces( result.getClass() ).toArray(
              new Class<?>[] {} ) ) {
            if ( interfacesToDelegate.contains( iface ) ) {
              result = forObject( result, interfacesToDelegate, UserGroupInformation.getCurrentUser().getUserName() );
              break;
            }
          }
        }
        return result;
      }

      @Override
      public String toString() {
        return delegate.getClass().getCanonicalName() + "." + method.toString();
      }
    };
    try {
      return runAsUser( action, user );
    } catch ( Exception e ) {
      if ( e instanceof InvocationTargetException ) {
        throw e.getCause();
      }
      throw e;
    }
  }
}
