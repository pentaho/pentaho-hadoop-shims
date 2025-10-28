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


package org.pentaho.hadoop.shim;

import org.pentaho.di.core.plugins.KettleURLClassLoader;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

/**
 * Loads classes for a Hadoop configuration by looking for resources in the configuration directory first before
 * checking its parent class loader.
 */
public class HadoopConfigurationClassLoader extends KettleURLClassLoader {

  private Set<String> loadClassesFromParent;

  /**
   * Create a class loader capable of loading classes for a Hadoop configuration.
   *
   * @param urls           Paths to directories or jars to load resources from
   * @param parent         Parent class loader to delegate loading of resources to if we cannot find them within the
   *                       list of URLs
   * @param ignoredClasses Classes or package names to explicitly delegate loading to the parent class loader
   */
  public HadoopConfigurationClassLoader( URL[] urls, ClassLoader parent, String... ignoredClasses ) {
    super( urls, parent );
    if ( parent == null ) {
      throw new NullPointerException( "parent ClassLoader is required" );
    }

    loadClassesFromParent = new HashSet<String>();
    if ( ignoredClasses != null ) {
      loadClassesFromParent.addAll( Arrays.asList( ignoredClasses ) );
    }
    loadClassesFromParent.add( "org.apache.commons.log" );
    loadClassesFromParent.add( "org.apache.logging.log4j" );
    loadClassesFromParent.add( "org.slf4j" );
  }

  /**
   * Determine if a class should be ignored by this class loader and loading of it should be delegated to its parent.
   *
   * @param name Name of class
   * @return {@code true} if the class should be ignored by this class loader
   */
  protected boolean ignoreClass( String name ) {
    if ( name == null || loadClassesFromParent.contains( name ) ) {
      return true;
    }
    for ( String prefix : loadClassesFromParent ) {
      if ( name.startsWith( prefix ) ) {
        return true;
      }
    }
    return false;
  }

  @Override
  protected synchronized Class<?> loadClass( String name, boolean resolve ) throws ClassNotFoundException {
    if ( ignoreClass( name ) ) {
      Class<?> c = Class.forName( name, false, getParent() );
      if ( resolve ) {
        resolveClass( c );
      }
      return c;
    }
    // Check for a previously loaded class
    Class<?> c = findLoadedClass( name );
    if ( c == null ) {
      // Try to load it from ourself first
      try {
        c = findClass( name );
      } catch ( ClassNotFoundException ex ) {
        // If we can't find the class check the parent class loader
        try {
          c = Class.forName(name, false, getParent());
        } catch ( ClassCircularityError e ) {
          // Ignore circularity errors, they are expected in some cases
        }
      }
    }
    // Resolve the class as needed
    if ( resolve ) {
      resolveClass( c );
    }
    return c;
  }

  @Override
  public URL getResource( String name ) {
    URL url = null;
    if ( url == null ) {
      url = findResource( name );
    }
    if ( url == null ) {
      url = super.getResource( name );
    }
    return url;
  }

  @Override
  public Enumeration<URL> getResources( String name ) throws IOException {
    final Enumeration<URL> myResources = findResources( name );
    final Enumeration<URL> parentResources = getParent().getResources( name );

    return new Enumeration<URL>() {
      @Override
      public boolean hasMoreElements() {
        return myResources.hasMoreElements() || parentResources.hasMoreElements();
      }

      @Override
      public URL nextElement() {
        if ( myResources.hasMoreElements() ) {
          return myResources.nextElement();
        }
        return parentResources.nextElement();
      }
    };
  }

  /**
   * Generates a "java.class.path" compatible String representing the class path available through this class loader.
   *
   * @return A "java.class.path" compatible String
   */
  public String generateClassPathString() {
    StringBuilder sb = new StringBuilder();

    for ( URL url : getURLs() ) {
      if ( sb.length() != 0 ) {
        sb.append( File.pathSeparator );
      }
      sb.append( url.getFile() );
    }

    return sb.toString();
  }
}
