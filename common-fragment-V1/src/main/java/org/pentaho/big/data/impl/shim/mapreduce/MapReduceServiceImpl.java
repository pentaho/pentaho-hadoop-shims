/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.impl.shim.mapreduce;

import com.google.common.annotations.VisibleForTesting;

import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.plugins.LifecyclePluginType;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hadoop.PluginPropertiesUtil;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceExecutionException;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceJarInfo;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceJobBuilder;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceJobSimple;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceService;
import org.pentaho.hadoop.shim.api.mapreduce.PentahoMapReduceJobBuilder;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

/**
 * Created by bryan on 12/1/15.
 */
public class MapReduceServiceImpl implements MapReduceService {
  public static final Class<?> PKG = MapReduceServiceImpl.class;
  private final NamedCluster namedCluster;
  private final HadoopShim hadoopShim;
  private final ExecutorService executorService;
  private final List<TransformationVisitorService> visitorServices;
  private final PluginPropertiesUtil pluginPropertiesUtil;
  private final PluginRegistry pluginRegistry;

  public MapReduceServiceImpl( NamedCluster namedCluster, HadoopShim hadoopShim,
                               ExecutorService executorService, List<TransformationVisitorService> visitorServices ) {
    this( namedCluster, hadoopShim, executorService, new PluginPropertiesUtil(),
      PluginRegistry.getInstance(), visitorServices );
  }

  public MapReduceServiceImpl( NamedCluster namedCluster, HadoopShim hadoopShim,
                               ExecutorService executorService,
                               PluginPropertiesUtil pluginPropertiesUtil, PluginRegistry pluginRegistry,
                               List<TransformationVisitorService> visitorServices ) {
    this.namedCluster = namedCluster;
    this.hadoopShim = hadoopShim;
    this.executorService = executorService;
    this.pluginPropertiesUtil = pluginPropertiesUtil;
    this.pluginRegistry = pluginRegistry;
    this.visitorServices = visitorServices;
  }

  @Override
  public MapReduceJobSimple executeSimple( URL resolvedJarUrl, String driverClass, final String commandLineArgs )
    throws MapReduceExecutionException {
    final Class<?> mainClass = locateDriverClass( driverClass, resolvedJarUrl, hadoopShim );
    return new FutureMapReduceJobSimpleImpl( executorService, mainClass, commandLineArgs );
  }

  @Override
  public MapReduceJobBuilder createJobBuilder( final LogChannelInterface log, VariableSpace variableSpace ) {
    return new MapReduceJobBuilderImpl( namedCluster, hadoopShim, log, variableSpace );
  }

  @Override
  public PentahoMapReduceJobBuilder createPentahoMapReduceJobBuilder( LogChannelInterface log,
                                                                      VariableSpace variableSpace )
    throws IOException {
    PluginInterface pluginInterface =
      pluginRegistry.findPluginWithId( LifecyclePluginType.class, "HadoopSpoonPlugin" );
    Properties pmrProperties;
    try {
      pmrProperties = pluginPropertiesUtil.loadPluginProperties( pluginInterface );
      return new PentahoMapReduceJobBuilderImpl( namedCluster, hadoopShim, log, variableSpace, pluginInterface,
        pmrProperties, visitorServices );
    } catch ( KettleFileException e ) {
      throw new IOException( e );
    }
  }

  @Override
  public MapReduceJarInfo getJarInfo( URL resolvedJarUrl ) throws IOException, ClassNotFoundException {
    ClassLoader classLoader = getClass().getClassLoader();
    List<Class<?>> classesInJarWithMain = getClassesInJarWithMain( resolvedJarUrl.toExternalForm(), classLoader );
    List<String> classNamesInJarWithMain = new ArrayList<>( classesInJarWithMain.size() );
    for ( Class<?> aClass : classesInJarWithMain ) {
      classNamesInJarWithMain.add( aClass.getCanonicalName() );
    }

    final List<String> finalClassNamesInJarWithMain = Collections.unmodifiableList( classNamesInJarWithMain );

    Class<?> mainClassFromManifest = null;
    try {
      mainClassFromManifest = getMainClassFromManifest( resolvedJarUrl, classLoader );
    } catch ( Exception e ) {
      // Ignore
    }

    final String mainClassName = mainClassFromManifest != null ? mainClassFromManifest.getCanonicalName() : null;

    return new MapReduceJarInfo() {
      @Override
      public List<String> getClassesWithMain() {
        return finalClassNamesInJarWithMain;
      }

      @Override
      public String getMainClass() {
        return mainClassName;
      }
    };
  }

  public void addTransformationVisitorService( TransformationVisitorService service ) {
    visitorServices.add( service );
  }

  @VisibleForTesting
  Class<?> locateDriverClass( String driverClass, final URL resolvedJarUrl, final HadoopShim shim )
    throws MapReduceExecutionException {
    try {
      if ( Utils.isEmpty( driverClass ) ) {
        Class<?> mainClass = getMainClassFromManifest( resolvedJarUrl, shim.getClass().getClassLoader() );
        if ( mainClass == null ) {
          List<Class<?>> mainClasses =
            getClassesInJarWithMain( resolvedJarUrl.toExternalForm(), shim.getClass().getClassLoader() );
          if ( mainClasses.size() == 1 ) {
            return mainClasses.get( 0 );
          } else if ( mainClasses.isEmpty() ) {
            throw new MapReduceExecutionException( BaseMessages.getString( PKG, "MapReduceServiceImpl.DriverClassNotSpecified" ) );
          } else {
            throw new MapReduceExecutionException( BaseMessages.getString( PKG, "MapReduceServiceImpl.MultipleDriverClasses" ) );
          }
        }
        return mainClass;
      } else {
        return getClassByName( driverClass, resolvedJarUrl, shim.getClass().getClassLoader() );
      }
    } catch ( MapReduceExecutionException mrEx ) {
      throw mrEx;
    } catch ( Exception e ) {
      throw new MapReduceExecutionException( e );
    }
  }

  private List<Class<?>> getClassesInJarWithMain( String jarUrl, ClassLoader parentClassloader )
          throws MalformedURLException {
    ArrayList<Class<?>> mainClasses = new ArrayList<Class<?>>();
    List<Class<?>> allClasses = getClassesInJar( jarUrl, parentClassloader );
    for ( Class<?> clazz : allClasses ) {
      try {
        Method mainMethod = clazz.getMethod( "main", new Class[] { String[].class } );
        if ( Modifier.isStatic( mainMethod.getModifiers() ) ) {
          mainClasses.add( clazz );
        }
      } catch ( Throwable ignored ) {
        // Ignore classes without main() methods
      }
    }
    return mainClasses;
  }

  private Class<?> getMainClassFromManifest( URL jarUrl, ClassLoader parentClassLoader )
          throws IOException, ClassNotFoundException {
    JarFile jarFile = getJarFile( jarUrl, parentClassLoader );
    try {
      Manifest manifest = jarFile.getManifest();
      String className = manifest == null ? null : manifest.getMainAttributes().getValue( "Main-Class" );
      return loadClassByName( className, jarUrl, parentClassLoader );
    } finally {
      jarFile.close();
    }
  }

  private JarFile getJarFile( final URL jarUrl, final ClassLoader parentClassLoader ) throws IOException {
    if ( jarUrl == null || parentClassLoader == null ) {
      throw new NullPointerException();
    }
    JarFile jarFile;
    try {
      jarFile = new JarFile( new File( jarUrl.toURI() ) );
    } catch ( URISyntaxException ex ) {
      throw new IOException( "Error locating jar: " + jarUrl );
    } catch ( IOException ex ) {
      throw new IOException( "Error opening job jar: " + jarUrl, ex );
    }
    return jarFile;
  }

  private Class<?> loadClassByName( final String className, final URL jarUrl, final ClassLoader parentClassLoader )
          throws ClassNotFoundException {
    if ( className != null ) {
      URLClassLoader cl = new URLClassLoader( new URL[] { jarUrl }, parentClassLoader );
      Class<?> clazz = cl.loadClass( className.replaceAll( "/", "." ) );
      try {
        cl.close();
      } catch ( IOException e ) {
      }
      return clazz;
    } else {
      return null;
    }
  }

  private Class<?> getClassByName( String className, URL jarUrl, ClassLoader parentClassLoader )
          throws IOException, ClassNotFoundException {
    JarFile jarFile = getJarFile( jarUrl, parentClassLoader );
    try {
      return loadClassByName( className, jarUrl, parentClassLoader );
    } finally {
      jarFile.close();
    }
  }

  private List<Class<?>> getClassesInJar( String jarUrl, ClassLoader parentClassloader )
          throws MalformedURLException {
    ArrayList<Class<?>> classes = new ArrayList<Class<?>>();
    URL url = new URL( jarUrl );
    URL[] urls = new URL[] { url };
    try ( URLClassLoader loader = new URLClassLoader( urls, getClass().getClassLoader() );
          JarInputStream jarFile = new JarInputStream( new FileInputStream( new File( url.toURI() ) ) ) ) {
      while ( true ) {
        JarEntry jarEntry = jarFile.getNextJarEntry();
        if ( jarEntry == null ) {
          break;
        }
        if ( jarEntry.getName().endsWith( ".class" ) ) {
          String className = jarEntry.getName().substring( 0, jarEntry.getName().indexOf( ".class" ) ).replaceAll( "/", "\\." );
          classes.add( loader.loadClass( className ) );
        }
      }
    } catch ( IOException e ) {
    } catch ( ClassNotFoundException e ) {
    } catch ( URISyntaxException e ) {
    }
    return classes;
  }

}
