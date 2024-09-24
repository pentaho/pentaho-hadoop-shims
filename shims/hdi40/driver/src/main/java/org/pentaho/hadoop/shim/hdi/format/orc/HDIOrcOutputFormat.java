/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2021 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.hdi.format.orc;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.TypeDescription;
import org.pentaho.hadoop.shim.HadoopShim;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.common.format.S3NCredentialUtils;
import org.pentaho.hadoop.shim.common.format.orc.OrcSchemaConverter;
import org.pentaho.hadoop.shim.common.format.orc.PentahoOrcOutputFormat;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;

public class HDIOrcOutputFormat extends PentahoOrcOutputFormat {

  private final org.pentaho.hadoop.shim.api.internal.Configuration pentahoConf;
  private HadoopShim shim;

  public HDIOrcOutputFormat( NamedCluster namedCluster ) {
    super( namedCluster );
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    shim = new HadoopShim();
    pentahoConf = shim.createConfiguration( namedCluster );
  }

  @Override
  public IPentahoRecordWriter createRecordWriter() throws RuntimeException {
    logger.logDetailed( "Initializing HDI Orc Writer" );
    if ( fields == null ) {
      throw new IllegalStateException( "Invalid state.  The fields to write are null" );
    }
    if ( outputFilename == null ) {
      throw new IllegalStateException( "Invalid state.  The outputFileName is null" );
    }
    OrcSchemaConverter converter = new OrcSchemaConverter();
    TypeDescription schema = converter.buildTypeDescription( fields );

    try {
      return new HDIOrcRecordWriter( fields, schema, outputFilename, conf,
              (FileSystem) shim.getFileSystem( pentahoConf ) );
    } catch ( IOException e ) {
      throw new IllegalStateException( e );
    }
  }

  @Override
  public void setOutputFile( String file, boolean override ) throws Exception {
    this.outputFilename = S3NCredentialUtils.scrubFilePathIfNecessary( file );
    S3NCredentialUtils util = new S3NCredentialUtils();
    util.applyS3CredentialsToHadoopConfigurationIfNecessary( file, conf );
    Path outputFile = new Path( outputFilename );
    FileSystem fs = (FileSystem) shim.getFileSystem( pentahoConf ).getDelegate();
    if ( fs.exists( outputFile ) ) {
      if ( override ) {
        fs.delete( outputFile, true );
      } else {
        throw new FileAlreadyExistsException( file );
      }
    }
  }
}
