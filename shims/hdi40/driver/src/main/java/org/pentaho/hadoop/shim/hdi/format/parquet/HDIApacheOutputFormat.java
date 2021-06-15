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
package org.pentaho.hadoop.shim.hdi.format.parquet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.pentaho.hadoop.shim.HadoopShim;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.common.format.S3NCredentialUtils;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoApacheOutputFormat;

import java.nio.file.FileAlreadyExistsException;

public class HDIApacheOutputFormat extends PentahoApacheOutputFormat {

  private HadoopShim shim;
  private org.pentaho.hadoop.shim.api.internal.Configuration pentahoConf;

  @SuppressWarnings( "squid:S1874" )
  public HDIApacheOutputFormat( NamedCluster namedCluster ) {
    super( namedCluster );
    inClassloader( () -> {
      shim = new HadoopShim();
      if ( namedCluster != null ) {
        pentahoConf = shim.createConfiguration( namedCluster );
      }
    } );
  }

  @Override
  public void setOutputFile( String file, boolean override ) throws Exception {
    inClassloader( () -> {
      S3NCredentialUtils util = new S3NCredentialUtils();
      util.applyS3CredentialsToHadoopConfigurationIfNecessary( file, job.getConfiguration() );
      outputFile = new Path( S3NCredentialUtils.scrubFilePathIfNecessary( file ) );
      FileSystem fs = (FileSystem) shim.getFileSystem( pentahoConf ).getDelegate();
      if ( fs.exists( outputFile ) ) {
        if ( override ) {
          fs.delete( outputFile, true );
        } else {
          throw new FileAlreadyExistsException( file );
        }
      }
      this.outputFile = new Path( fs.getUri().toString() + this.outputFile.toUri().getPath() );
      this.outputFile = fs.makeQualified( this.outputFile );
      this.job.getConfiguration().set( "mapreduce.output.fileoutputformat.outputdir", this.outputFile.toString() );
    } );
  }
}
