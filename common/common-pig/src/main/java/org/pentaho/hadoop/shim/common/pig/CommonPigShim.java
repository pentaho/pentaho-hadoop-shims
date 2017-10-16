/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package org.pentaho.hadoop.shim.common.pig;

import org.apache.commons.io.FileUtils;
import org.apache.pig.ExecType;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.PropertiesUtil;
import org.apache.pig.tools.parameters.ParameterSubstitutionPreprocessor;
import org.apache.pig.tools.parameters.ParseException;
import org.osgi.framework.BundleContext;
import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.common.ShimUtils;
import org.pentaho.hadoop.shim.spi.PigShim;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public abstract class CommonPigShim implements PigShim {
  private static final String[] EMPTY_STRING_ARRAY = new String[ 0 ];


  private BundleContext bundleContext;

  public BundleContext getBundleContext() {
    return bundleContext;
  }

  public void setBundleContext( BundleContext bundleContext ) {
    this.bundleContext = bundleContext;
  }

  private enum ExternalPigJars {

    PIG( "pig" ),
    AUTOMATON( "automaton" ),
    ANTLR( "antlr-runtime" ),
    GUAVA( "guava" ),
    JACKSON_CORE( "jackson-core-asl" ),
    JACKSON_MAPPER( "jackson-mapper-asl" ),
    JODATIME( "joda-time" );

    private final String jarName;

    ExternalPigJars( String jarName ) {
      this.jarName = jarName;
    }

    public String getJarName() {
      return jarName;
    }

  }

  public void addExternalJarsToPigContext( PigContext pigContext ) throws MalformedURLException {
    File filesInsideBundle = new File( bundleContext.getBundle().getDataFile( "" ).getParent() );
    Iterator<File> filesIterator = FileUtils.iterateFiles( filesInsideBundle, new String[] { "jar" }, true );
    while ( filesIterator.hasNext() ) {
      File file = filesIterator.next();
      addMatchedJarToPigContext( pigContext, file );
    }
  }

  private void addMatchedJarToPigContext( PigContext pigContext, File jarFile ) throws MalformedURLException {
    String jarName = jarFile.getName();
    for ( ExternalPigJars externalPigJars : ExternalPigJars.values() ) {
      if ( jarName.startsWith( externalPigJars.getJarName() ) ) {
        String jarPath = jarFile.getAbsolutePath();
        pigContext.addJar( jarPath );
        break;
      }
    }
  }

  @Override
  public ShimVersion getVersion() {
    return new ShimVersion( 1, 0 );
  }

  @Override
  public boolean isLocalExecutionSupported() {
    return true;
  }

  @Override
  public void configure( Properties properties, Configuration configuration ) {
    PropertiesUtil.loadDefaultProperties( properties );
    if ( configuration != null ) {
      properties.putAll( ConfigurationUtil.toProperties( ShimUtils.asConfiguration( configuration ) ) );
      properties.setProperty( "pig.use.overriden.hadoop.configs", "true" );
    }
  }

  @Override
  public String substituteParameters( URL pigScript, List<String> paramList ) throws IOException, ParseException {
    final InputStream inStream = pigScript.openStream();
    StringWriter writer = new StringWriter();
    // do parameter substitution
    ParameterSubstitutionPreprocessor psp = new ParameterSubstitutionPreprocessor( 50 );
    psp.genSubstitutedFile( new BufferedReader( new InputStreamReader( inStream ) ),
      writer,
      paramList.size() > 0 ? paramList.toArray( EMPTY_STRING_ARRAY ) : null, null );
    return writer.toString();
  }

  /**
   * Convert {@link ExecutionMode} to {@link ExecType}
   *
   * @param mode Execution mode
   * @return Type of execution for mode
   */
  protected ExecType getExecType( ExecutionMode mode ) {
    switch( mode ) {
      case LOCAL:
        return ExecType.LOCAL;
      case MAPREDUCE:
        return ExecType.MAPREDUCE;
      default:
        throw new IllegalStateException( "unknown execution mode: " + mode );
    }
  }
}
