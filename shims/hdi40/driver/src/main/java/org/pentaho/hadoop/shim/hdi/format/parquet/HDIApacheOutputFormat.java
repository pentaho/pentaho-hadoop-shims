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
