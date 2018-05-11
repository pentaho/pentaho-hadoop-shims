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
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.osgi.framework.BundleContext;
import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.spi.SqoopShim;
import org.pentaho.hadoop.shim.common.ShimUtils;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

@SuppressWarnings( "deprecation" )
public class CommonSqoopShim implements SqoopShim {
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
      c.set( "tmpjars", getSqoopJarLocation(c) );
      return Sqoop.runTool( args, ShimUtils.asConfiguration( c ) );
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

}
