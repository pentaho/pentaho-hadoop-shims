/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019-2020 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.common.format.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.TypeDescription;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.hadoop.shim.ShimConfigsLoader;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.format.IOrcOutputField;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcOutputFormat;
import org.pentaho.hadoop.shim.api.format.org.pentaho.hadoop.shim.pvfs.api.PvfsHadoopBridgeFileSystemExtension;
import org.pentaho.hadoop.shim.common.ConfigurationProxy;
import org.pentaho.hadoop.shim.common.format.HadoopFormatBase;
import org.pentaho.hadoop.shim.common.format.S3NCredentialUtils;

import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Created by tkafalas on 11/3/2017.
 */
public class PentahoOrcOutputFormat extends HadoopFormatBase implements IPentahoOrcOutputFormat {

  private static final LogChannelInterface logger = LogChannel.GENERAL;
  private String outputFilename;
  private final Configuration conf;
  private COMPRESSION compression = COMPRESSION.NONE;
  private int compressSize = 0;
  private int stripeSize = DEFAULT_STRIPE_SIZE;
  private List<? extends IOrcOutputField> fields;

  public PentahoOrcOutputFormat() {
    this( null );
  }

  public PentahoOrcOutputFormat( NamedCluster namedCluster ) {
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    conf = new ConfigurationProxy();

    if ( namedCluster != null ) {
      // if named cluster is not defined, no need to add cluster resource configs
      BiConsumer<InputStream, String> consumer = ( is, filename ) -> conf.addResource( is, filename );
      ShimConfigsLoader.addConfigsAsResources( namedCluster, consumer );
    } else {
      conf.addResource( "hive-site.xml" );
    }
  }

  @Override public IPentahoRecordWriter createRecordWriter() {
    logger.logDetailed( "Initializing Orc Writer" );
    if ( fields == null ) {
      throw new IllegalStateException( "Invalid state.  The fields to write are null" );
    }
    if ( outputFilename == null ) {
      throw new IllegalStateException( "Invalid state.  The outputFileName is null" );
    }
    OrcSchemaConverter converter = new OrcSchemaConverter();
    TypeDescription schema = converter.buildTypeDescription( fields );

    return new PentahoOrcRecordWriter( fields, schema, outputFilename, conf );
  }

  @Override
  public void setFields( List<? extends IOrcOutputField> fields ) {
    this.fields = fields;
  }

  @Override public void setOutputFile( String file, boolean override ) throws Exception {
    this.outputFilename = S3NCredentialUtils.scrubFilePathIfNecessary( file );
    S3NCredentialUtils util = new S3NCredentialUtils();
    util.applyS3CredentialsToHadoopConfigurationIfNecessary( file, conf );
    Path outputFile = new Path( outputFilename );
    FileSystem fs = FileSystem.get( outputFile.toUri(), conf );
    if ( fs.exists( outputFile ) ) {
      if ( override ) {
        fs.delete( outputFile, true );
      } else {
        throw new FileAlreadyExistsException( file );
      }
    }


  }

  @Override public void setCompression( COMPRESSION compression ) {
    this.compression = compression;
    conf.set( COMPRESSION_KEY, compression.toString() );
    if ( compression == COMPRESSION.NONE ) {
      compressSize = 0;
      conf.unset( COMPRESS_SIZE_KEY );
    } else if ( compressSize == 0 ) {
      compressSize = DEFAULT_COMPRESS_SIZE;
      conf.set( COMPRESS_SIZE_KEY, Integer.toString( DEFAULT_COMPRESS_SIZE ) );
    }
  }

  @Override public void setStripeSize( int megabytes ) {
    if ( stripeSize > 0 ) {
      stripeSize = megabytes;
      conf.set( STRIPE_SIZE_KEY, Integer.toString( 1024 * 1024 * stripeSize ) );
    }
  }

  @Override
  public void setRowIndexStride( int numRows ) {
    if ( numRows > 0 ) {
      conf.set( CREATE_INDEX_KEY, "true" );
      conf.set( ROW_INDEX_STRIDE_KEY, Integer.toString( 1024 * 1024 * numRows ) );
    } else if ( numRows == 0 ) {
      conf.set( CREATE_INDEX_KEY, "false" );
      conf.unset( ROW_INDEX_STRIDE_KEY );
    }
  }

  @Override
  public void setCompressSize( int kilobytes ) {
    if ( kilobytes > 0 ) {
      compressSize = kilobytes;
      conf.set( COMPRESS_SIZE_KEY, Integer.toString( 1024 * compressSize ) );
    } else if ( kilobytes == 0 ) {
      compressSize = kilobytes;
      compression = COMPRESSION.NONE;
      conf.unset( COMPRESS_SIZE_KEY );
      conf.set( COMPRESSION_KEY, compression.toString() );
    }
  }

  public String generateAlias( String pvfsPath ) {
    return inClassloader( () -> {
        FileSystem fs = FileSystem.get( new URI( pvfsPath ), conf );
        if ( fs instanceof PvfsHadoopBridgeFileSystemExtension ) {
          return ( (PvfsHadoopBridgeFileSystemExtension) fs ).generateAlias( pvfsPath );
        } else {
          return null;
        }
      }
    );
  }
}
