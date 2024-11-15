/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.hadoop.shim.common.utils;

import java.lang.reflect.Field;
import java.net.URL;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

/**
 * User: Dzmitry Stsiapanau Date: 11/16/2015 Time: 12:48
 */

@SuppressWarnings( "unchecked" )
public class OverloadedServiceLoader<S> implements Iterable<S> {

  private static void fail( Class service, String msg, Throwable cause )
    throws ServiceConfigurationError {
    throw new ServiceConfigurationError( service.getName() + ": " + msg,
      cause );
  }

  private static void fail( Class service, String msg )
    throws ServiceConfigurationError {
    throw new ServiceConfigurationError( service.getName() + ": " + msg );
  }

  private static void fail( Class service, URL u, int line, String msg )
    throws ServiceConfigurationError {
    fail( service, u + ":" + line + ": " + msg );
  }

  public class OverloadedLazyIterator<S> implements OverloadedIterator<S> {

    public Iterator<S> originalLookupIterator;

    public OverloadedLazyIterator( Iterator<S> originalLookupIterator ) {
      this.originalLookupIterator = originalLookupIterator;
    }

    /**
     * Returns {@code true} if the iteration has more elements. (In other words, returns {@code true} if {@link #next}
     * would return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     */
    @Override public boolean hasNext() {
      return originalLookupIterator.hasNext();
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    @Override public S next() {
      return originalLookupIterator.next();
    }


    /**
     * Returns the next element in the iteration.
     *
     * @param args Types which are used in non-default constructor (Class<?>) followed by their values. For example: (
     *             String.class, "someString", Integer.class, 0 )
     * @return the next element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */

    public S next( Object... args ) {
      if ( args == null ) {
        return next();
      }
      if ( !hasNext() ) {
        throw new NoSuchElementException();
      }
      String cn = (String) getPrivateField( "nextName", originalLookupIterator );
      setPrivateField( "nextName", originalLookupIterator, null );
      Class<?> c = null;
      try {
        c = Class.forName( cn, false, (ClassLoader) getPrivateField( "loader", originalLookupIterator ) );
      } catch ( ClassNotFoundException x ) {
        fail( (Class<S>) getPrivateField( "service", originalLookupIterator ), "Provider " + cn + " not found" );
      }
      if ( !( (Class<S>) getPrivateField( "service", originalLookupIterator ) ).isAssignableFrom( c ) ) {
        fail( (Class) getPrivateField( "service", originalLookupIterator ), "Provider " + cn + " not a subtype" );
      }
      try {
        S p = ( (Class<S>) getPrivateField( "service", originalLookupIterator ) )
          .cast( c.getConstructor( getTypes( args ) ).newInstance( getArgs( args ) ) );
        ( (LinkedHashMap<String, S>) getPrivateField( "providers", loader ) ).put( cn, p );
        return p;
      } catch ( Throwable x ) {
        fail( (Class<S>) getPrivateField( "service", originalLookupIterator ),
          "Provider " + cn + " could not be instantiated: " + x, x );
      }
      throw new Error();          // This cannot happen
    }

    private Class<?>[] getTypes( Object[] args ) {
      int halfSize = args.length / 2;
      assert args.length % 2 == 0;
      Class<?>[] classes = new Class<?>[ halfSize ];
      for ( int i = 0; i < halfSize; i++ ) {
        classes[ i ] = (Class<?>) args[ i * 2 ];
      }
      return classes;
    }

    private Object[] getArgs( Object[] args ) {
      int halfSize = args.length / 2;
      assert args.length % 2 == 0;
      Object[] values = new Object[ halfSize ];
      for ( int i = 0; i < halfSize; i++ ) {
        values[ i ] = args[ i * 2 + 1 ];
      }
      return values;
    }

    /**
     * Removes from the underlying collection the last element returned by this iterator (optional operation).  This
     * method can be called only once per call to {@link #next}.  The behavior of an iterator is unspecified if the
     * underlying collection is modified while the iteration is in progress in any way other than by calling this
     * method.
     *
     * @throws UnsupportedOperationException if the {@code remove} operation is not supported by this iterator
     * @throws IllegalStateException         if the {@code next} method has not yet been called, or the {@code remove}
     *                                       method has already been called after the last call to the {@code next}
     *                                       method
     */
    @Override public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private ServiceLoader<S> loader;
  private OverloadedLazyIterator<S> iterator;

  public OverloadedServiceLoader( ServiceLoader<S> loader ) {
    this.loader = loader;
    reload();
  }

  private Object getPrivateField( String fieldName, Object obj ) {
    Field f = null;
    try {
      f = obj.getClass().getDeclaredField( fieldName );
      f.setAccessible( true );
      return f.get( obj );
    } catch ( NoSuchFieldException | IllegalAccessException e ) {
      e.printStackTrace();
    } finally {
      if ( f != null ) {
        f.setAccessible( false );
      }
    }
    return null;
  }

  private void setPrivateField( String fieldName, Object obj, Object value ) {
    Field f = null;
    try {
      f = obj.getClass().getDeclaredField( fieldName );
      f.setAccessible( true );
      f.set( obj, value );
    } catch ( NoSuchFieldException | IllegalAccessException e ) {
      e.printStackTrace();
    } finally {
      if ( f != null ) {
        f.setAccessible( false );
      }
    }
  }

  /**
   * Creates a new service loader for the given service type, using the extension class loader.
   * <p/>
   * <p> This convenience method simply locates the extension class loader, call it <tt><i>extClassLoader</i></tt>,
   * and then returns
   * <p/>
   * <blockquote><pre>
   * OverloadedServiceLoader.load(<i>service</i>, <i>extClassLoader</i>)</pre></blockquote>
   * <p/>
   * <p> If the extension class loader cannot be found then the system class loader is used; if there is no system
   * class loader then the bootstrap class loader is used.
   * <p/>
   * <p> This method is intended for use when only installed providers are desired.  The resulting service will only
   * find and load providers that have been installed into the current Java virtual machine; providers on the
   * application's class path will be ignored.
   *
   * @param service The interface or abstract class representing the service
   * @return A new service loader
   */
  public static <S> OverloadedServiceLoader<S> loadInstalled( Class<S> service ) {
    return new OverloadedServiceLoader<>( ServiceLoader.loadInstalled( service ) );
  }

  /**
   * Creates a new service loader for the given service type, using the current thread's {@linkplain
   * Thread#getContextClassLoader context class loader}.
   * <p/>
   * <p> An invocation of this convenience method of the form
   * <p/>
   * <blockquote><pre>
   * OverloadedServiceLoader.load(<i>service</i>)</pre></blockquote>
   * <p/>
   * is equivalent to
   * <p/>
   * <blockquote><pre>
   * OverloadedServiceLoader.load(<i>service</i>,
   *                    Thread.currentThread().getContextClassLoader())</pre></blockquote>
   *
   * @param service The interface or abstract class representing the service
   * @return A new service loader
   */
  public static <S> OverloadedServiceLoader<S> load( Class<S> service ) {
    return new OverloadedServiceLoader<>( ServiceLoader.load( service ) );
  }

  /**
   * Creates a new service loader for the given service type and class loader.
   *
   * @param service The interface or abstract class representing the service
   * @param loader  The class loader to be used to load provider-configuration files and provider classes, or
   *                <tt>null</tt> if the system class loader (or, failing that, the bootstrap class loader) is to be
   *                used
   * @return A new service loader
   */
  public static <S> OverloadedServiceLoader<S> load( Class<S> service, ClassLoader loader ) {
    return new OverloadedServiceLoader<>( ServiceLoader.load( service, loader ) );
  }

  /**
   * Clear this loader's provider cache so that all providers will be reloaded.
   * <p/>
   * <p> After invoking this method, subsequent invocations of the {@link #iterator() iterator} method will lazily look
   * up and instantiate providers from scratch, just as is done by a newly-created loader.
   * <p/>
   * <p> This method is intended for use in situations in which new providers can be installed into a running Java
   * virtual machine.
   */
  public void reload() {
    loader.reload();
    this.iterator = new OverloadedLazyIterator( (Iterator) getPrivateField( "lookupIterator", loader ) );
  }

  /**
   * Lazily loads the available providers of this loader's service.
   * <p/>
   * <p> The iterator returned by this method first yields all of the elements of the provider cache, in instantiation
   * order.  It then lazily loads and instantiates any remaining providers, adding each one to the cache in turn.
   * <p/>
   * <p> To achieve laziness the actual work of parsing the available provider-configuration files and instantiating
   * providers must be done by the iterator itself.  Its {@link java.util.Iterator#hasNext hasNext} and {@link
   * java.util.Iterator#next next} methods can therefore throw a {@link ServiceConfigurationError} if a
   * provider-configuration file violates the specified format, or if it names a provider class that cannot be found and
   * instantiated, or if the result of instantiating the class is not assignable to the service type, or if any other
   * kind of exception or error is thrown as the next provider is located and instantiated.  To write robust code it is
   * only necessary to catch {@link ServiceConfigurationError} when using a service iterator.
   * <p/>
   * <p> If such an error is thrown then subsequent invocations of the iterator will make a best effort to locate and
   * instantiate the next available provider, but in general such recovery cannot be guaranteed.
   * <p/>
   * <blockquote style="font-size: smaller; line-height: 1.2"><span style="padding-right: 1em; font-weight:
   * bold">Design Note</span> Throwing an error in these cases may seem extreme.  The rationale for this behavior is
   * that a malformed provider-configuration file, like a malformed class file, indicates a serious problem with the way
   * the Java virtual machine is configured or is being used.  As such it is preferable to throw an error rather than
   * try to recover or, even worse, fail silently.</blockquote>
   * <p/>
   * <p> The iterator returned by this method does not support removal. Invoking its {@link java.util.Iterator#remove()
   * remove} method will cause an {@link UnsupportedOperationException} to be thrown.
   *
   * @return An iterator that lazily loads providers for this loader's service
   */
  @Override public Iterator<S> iterator() {
    return new OverloadedIterator<S>() {

      Iterator<Map.Entry<String, S>> knownProviders =
        ( (LinkedHashMap<String, S>) getPrivateField( "providers", loader ) ).entrySet().iterator();

      @Override public boolean hasNext() {
        if ( knownProviders.hasNext() ) {
          return true;
        }
        return iterator.hasNext();
      }

      @Override public S next() {
        if ( knownProviders.hasNext() ) {
          return knownProviders.next().getValue();
        }
        return iterator.next();
      }

      @Override public S next( Object... args ) {
        if ( knownProviders.hasNext() ) {
          return knownProviders.next().getValue();
        }
        return iterator.next( args );
      }

      @Override public void remove() {
        throw new UnsupportedOperationException();
      }

    };
  }

  /**
   * Returns a string describing this service.
   *
   * @return A descriptive string
   */
  public String toString() {
    return "org.pentaho.hadoop.shim.common.utils.OverloadedServiceLoader[" + super.toString() + "]";
  }
}
