package org.pentaho.hadoop.shim.mapr31.authorization;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;

import org.apache.commons.lang.ClassUtils;
import org.apache.hadoop.security.UserGroupInformation;

public class KerberosInvocationHandler<T> implements InvocationHandler {
//  private static final Logger logger = Logger.getLogger( UserSpoofingKerberosInvocationHandler.class );
  private final LoginContext loginContext;
  private final T delegate;
  private final Set<Class<?>> interfacesToDelegate;
  private final Set<Method> methodsWithoutGetUser;
  private final String user;
  private final boolean isRoot;

  public KerberosInvocationHandler( LoginContext loginContext, T delegate ) {
    this( loginContext, delegate, new HashSet<Class<?>>(), null, false );
  }

  public KerberosInvocationHandler( LoginContext loginContext, T delegate,
      Set<Class<?>> interfacesToDelegate ) {
    this( loginContext, delegate, interfacesToDelegate, null, false );
  }

  public KerberosInvocationHandler( LoginContext loginContext, T delegate,
      Set<Class<?>> interfacesToDelegate, String user, boolean isRoot ) {
    this.loginContext = loginContext;
    this.delegate = delegate;
    this.interfacesToDelegate = interfacesToDelegate;
    this.user = user;
    this.methodsWithoutGetUser = new HashSet<Method>();
    this.isRoot = isRoot;
  }

  public static <T> T forObject( LoginContext loginContext, T delegate, Set<Class<?>> interfacesToDelegate ) {
    return forObject( loginContext, delegate, interfacesToDelegate, null, false );
  }

  @SuppressWarnings( "unchecked" )
  public static <T> T forObject( LoginContext loginContext, T delegate, Set<Class<?>> interfacesToDelegate,
      String user, boolean isRoot ) {
    return (T) Proxy.newProxyInstance( delegate.getClass().getClassLoader(), (Class<?>[]) ClassUtils.getAllInterfaces(
        delegate.getClass() ).toArray( new Class<?>[] {} ), new KerberosInvocationHandler<Object>(
        loginContext, delegate, interfacesToDelegate, user, isRoot ) );
  }

  private Method getMethodForUserName( Method originalMethod ) {
    if ( !methodsWithoutGetUser.contains( originalMethod ) ) {
      try {
        return delegate.getClass().getMethod( originalMethod.getName() + "GetUser", originalMethod.getParameterTypes() );
      } catch ( Exception e ) {
        methodsWithoutGetUser.add( originalMethod );
      }
    }
    return null;
  }

  private <RunType> RunType runAsUser( PrivilegedExceptionAction<RunType> action, String user, boolean isRoot )
    throws Throwable {
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader( KerberosInvocationHandler.class.getClassLoader() );
      return Subject.doAs( loginContext.getSubject(), action );
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
              result =
                  forObject( loginContext, result, interfacesToDelegate, UserGroupInformation.getCurrentUser()
                      .getUserName(), isRoot );
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
    String impersonateUser = null;
    if ( user == null ) {
      Method methodForUsername = getMethodForUserName( method );
      if ( methodForUsername != null ) {
        String username = (String) methodForUsername.invoke( delegate, args );
        if ( username != null && username.length() > 0 ) {
          impersonateUser = username;
        }
      }
    } else {
      impersonateUser = user;
    }
    try {
      return runAsUser( action, impersonateUser, isRoot );
    } catch ( Exception e ) {
      if ( e instanceof InvocationTargetException ) {
        throw e.getCause();
      }
      throw e;
    }
  }
}
