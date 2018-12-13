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

package org.pentaho.hadoop.shim.common.sqoop;

import com.cloudera.sqoop.Sqoop;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import com.yammer.metrics.core.MetricsRegistry;
import io.netty.channel.Channel;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.htrace.Trace;
import org.apache.zookeeper.ZooKeeper;
import org.osgi.framework.BundleContext;
import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.spi.SqoopShim;
import org.pentaho.hadoop.shim.common.ShimUtils;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

@SuppressWarnings( "deprecation" )
public class CommonSqoopShim implements SqoopShim {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger( CommonSqoopShim.class );
  private static final String TMPJARS = "tmpjars";

  private BundleContext bundleContext;

  public BundleContext getBundleContext() {
    return bundleContext;
  }

  public void setBundleContext( BundleContext bundleContext ) {
    this.bundleContext = bundleContext;
  }

  @Override
  public ShimVersion getVersion() {
    return new ShimVersion( 1, 0 );
  }

  @Override
  public int runTool( String[] args, Configuration c ) {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    String tmpPropertyHolder = System.getProperty( "hadoop.alt.classpath" );
    try {
      System.setProperty( "hadoop.alt.classpath", createHadoopAltClasspath() );
      c.set( TMPJARS, getSqoopJarLocation( c ) );
      if ( args.length > 0 && Arrays.asList( args ).contains( "--hbase-table" ) ) {
          addHbaseDependencyJars( c, HConstants.class, ClientProtos.class, Put.class,
                  CompatibilityFactory.class, TableMapper.class, ZooKeeper.class,
                  Channel.class, Message.class, Lists.class, Trace.class, MetricsRegistry.class
          );
      }
      return Sqoop.runTool( args, ShimUtils.asConfiguration( c ) );
    } catch ( IOException e ) {
      e.printStackTrace();
      return -1;
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
      if ( tmpPropertyHolder == null ) {
        System.clearProperty( "hadoop.alt.classpath" );
      } else {
        System.setProperty( "hadoop.alt.classpath", tmpPropertyHolder );
      }
    }
  }

  private String createHadoopAltClasspath() {
    File filesInsideBundle = new File( bundleContext.getBundle().getDataFile( "" ).getParent() );
    Iterator<File> filesIterator = FileUtils.iterateFiles( filesInsideBundle, new String[] { "jar" }, true );

    StringBuilder sb = new StringBuilder();

    while ( filesIterator.hasNext() ) {
      File file = filesIterator.next();
      String name = file.getName();
      if ( name.startsWith( "hadoop-common" )
        || name.startsWith( "hadoop-mapreduce-client-core" )
        || name.startsWith( "hadoop-core" )
        || name.startsWith( "sqoop" ) ) {
        sb.append( file.getAbsolutePath() );
        sb.append( File.pathSeparator );
      }
    }

    return sb.toString();
  }

  private String getSqoopJarLocation( Configuration c ) {
    File filesInsideBundle = new File( bundleContext.getBundle().getDataFile( "" ).getParent() );
    Iterator<File> filesIterator = FileUtils.iterateFiles( filesInsideBundle, new String[] { "jar" }, true );

    StringBuilder sb = new StringBuilder();

    while ( filesIterator.hasNext() ) {
      File file = filesIterator.next();
      String name = file.getName();
      if ( name.startsWith( "sqoop" ) ) {
        sb.append( file.getAbsolutePath() );
      }
    }

    try {
      FileSystem fs = FileSystem.getLocal( ShimUtils.asConfiguration( c ) );
      return new Path( sb.toString() ).makeQualified( fs ).toString();
    } catch ( IOException e ) {
      e.printStackTrace();
    }
    return sb.toString();
  }

  public void addHbaseDependencyJars(Configuration conf, Class... classes )
    throws IOException {
    List<String> classNames = new ArrayList<String>();
    for ( Class clazz : classes ) {
      classNames.add( clazz.getCanonicalName().replace( ".", "/" ) + ".class" );
    }
    Set<String> tmpjars = new HashSet<String>();
    if ( conf.get( TMPJARS ) != null ) {
      tmpjars.addAll( Arrays.asList( conf.get( TMPJARS ).split( "," ) ) );
}
    File filesInsideBundle = new File( bundleContext.getBundle().getDataFile( "" ).getParent() );
    Iterator<File> filesIterator = FileUtils.iterateFiles( filesInsideBundle, new String[] { "jar" }, true );

    getOut:
    while ( filesIterator.hasNext() ) {
      File file = filesIterator.next();
      ZipFile zip = new ZipFile( file );
      // Process the jar file.

      try {
        // Loop through the jar entries and print the name of each one.

        for ( Enumeration list = zip.entries(); list.hasMoreElements(); ) {
          ZipEntry entry = (ZipEntry) list.nextElement();
          System.out.println( entry.getName() );
          if ( !entry.isDirectory() && entry.getName().endsWith( ".class" ) ) {
            ListIterator<String> classNameIterator = classNames.listIterator();
            while ( classNameIterator.hasNext() ) {
              if ( entry.getName().endsWith( classNameIterator.next() ) ) {
                // If here we found a class in this jar, add the jar to the list, and delete the class from classNames.
                tmpjars.add( file.toURI().toURL().toString() );
                classNameIterator.remove();
                if ( classNames.size() == 0 ) {
                  break getOut;
                }
              }
            }
          }
        }
      } finally {
        zip.close();
      }
    }

    StringBuilder sb = new StringBuilder();
    if ( tmpjars.size() > 0 ) {
      for ( String jarPath : tmpjars ) {
        sb.append( "," ).append( jarPath );
      }
      conf.set( TMPJARS, sb.toString().substring( 1 ) );
    }
  }
}