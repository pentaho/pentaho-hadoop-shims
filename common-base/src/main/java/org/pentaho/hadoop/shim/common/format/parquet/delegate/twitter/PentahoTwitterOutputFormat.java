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

package org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetRecordWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.pentaho.hadoop.shim.api.format.org.pentaho.hadoop.shim.pvfs.api.PvfsHadoopBridgeFileSystemExtension;

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.hadoop.shim.api.format.IParquetOutputField;
import org.pentaho.hadoop.shim.api.format.IPentahoParquetOutputFormat;
import org.pentaho.hadoop.shim.common.format.HadoopFormatBase;
import org.pentaho.hadoop.shim.common.format.S3NCredentialUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;

/**
 * Created by Vasilina_Terehova on 8/3/2017.
 */
public class PentahoTwitterOutputFormat extends HadoopFormatBase implements IPentahoParquetOutputFormat {

  private static final Logger logger = LogManager.getLogger( PentahoTwitterOutputFormat.class );

  private Job job;
  private Path outputFile;
  private List<? extends IParquetOutputField> outputFields;

  public PentahoTwitterOutputFormat() {
    logger.info( "We are initializing parquet output format" );

    inClassloader( () -> {
      Configuration conf = createConfigurationWithClassLoader( null, getClass().getClassLoader() );
      job = Job.getInstance( conf );

      job.getConfiguration().set( ParquetOutputFormat.ENABLE_JOB_SUMMARY, "false" );
      ParquetOutputFormat.setEnableDictionary( job, false );
    } );
  }

  @Override
  public void setFields( List<? extends IParquetOutputField> fields ) throws Exception {
    this.outputFields = fields;
  }

  @Override
  public void setOutputFile( String file, boolean override ) throws Exception {
    inClassloader( () -> {
      S3NCredentialUtils util = new S3NCredentialUtils();
      util.applyS3CredentialsToHadoopConfigurationIfNecessary( file, job.getConfiguration() );
      outputFile = new Path( S3NCredentialUtils.scrubFilePathIfNecessary( file ) );
      FileSystem fs = FileSystem.get( outputFile.toUri(), job.getConfiguration() );
      if ( fs.exists( outputFile ) ) {
        if ( override ) {
          fs.delete( outputFile, true );
        } else {
          throw new FileAlreadyExistsException( file );
        }
      }
      ParquetOutputFormat.setOutputPath( job, outputFile.getParent() );
    } );
  }

  @Override
  public void setVersion( VERSION version ) throws Exception {
    inClassloader( () -> {
      ParquetProperties.WriterVersion writerVersion;
      switch ( version ) {
        case VERSION_1_0:
          writerVersion = ParquetProperties.WriterVersion.PARQUET_1_0;
          break;
        case VERSION_2_0:
          writerVersion = ParquetProperties.WriterVersion.PARQUET_2_0;
          break;
        default:
          writerVersion = ParquetProperties.WriterVersion.PARQUET_2_0;
          break;
      }
      job.getConfiguration().set( ParquetOutputFormat.WRITER_VERSION, writerVersion.toString() );
    } );
  }

  @Override
  public void setCompression( COMPRESSION comp ) throws Exception {
    inClassloader( () -> {
      CompressionCodecName codec;
      switch ( comp ) {
        case SNAPPY:
          codec = CompressionCodecName.SNAPPY;
          break;
        case GZIP:
          codec = CompressionCodecName.GZIP;
          break;
        case LZO:
          codec = CompressionCodecName.LZO;
          break;
        default:
          codec = CompressionCodecName.UNCOMPRESSED;
          break;
      }
      ParquetOutputFormat.setCompression( job, codec );
    } );
  }

  @Override
  public void enableDictionary( boolean useDictionary ) throws Exception {
    inClassloader( () -> ParquetOutputFormat.setEnableDictionary( job, useDictionary ) );
  }

  @Override
  public void setRowGroupSize( int size ) throws Exception {
    inClassloader( () -> ParquetOutputFormat.setBlockSize( job, size ) );
  }

  @Override
  public void setDataPageSize( int size ) throws Exception {
    inClassloader( () -> ParquetOutputFormat.setPageSize( job, size ) );
  }

  @Override
  public void setDictionaryPageSize( int size ) throws Exception {
    inClassloader( () -> ParquetOutputFormat.setDictionaryPageSize( job, size ) );
  }

  @Override
  public IPentahoRecordWriter createRecordWriter() throws Exception {
    if ( outputFile == null ) {
      throw new IllegalStateException( "Output file is not defined" );
    }
    if ( ( outputFields == null ) || ( outputFields.isEmpty() ) ) {
      throw new IllegalStateException( "Schema is not defined" );
    }

    return inClassloader( () -> {
      FixedParquetOutputFormat nativeParquetOutputFormat
        = new FixedParquetOutputFormat( new PentahoParquetWriteSupport( outputFields ) );

      TaskAttemptID taskAttemptID = new TaskAttemptID( "qq", 111, TaskType.MAP, 11, 11 );
      TaskAttemptContextImpl task = new TaskAttemptContextImpl( job.getConfiguration(), taskAttemptID );
      try {

        ParquetRecordWriter<RowMetaAndData> recordWriter
          = ( ParquetRecordWriter<RowMetaAndData> ) nativeParquetOutputFormat.getRecordWriter( task );
        return new PentahoParquetRecordWriter( recordWriter, task );
      } catch ( IOException e ) {
        throw new IllegalStateException( "Some error accessing parquet files", e );
      } catch ( InterruptedException e ) {
        // logging here
        Thread.currentThread().interrupt();
        throw new IllegalStateException( "This should never happen " + e );
      }
    } );
  }

  public class FixedParquetOutputFormat extends ParquetOutputFormat<RowMetaAndData> {
    public FixedParquetOutputFormat( PentahoParquetWriteSupport writeSupport ) {
      super( writeSupport );
    }

    @Override
    public Path getDefaultWorkFile( TaskAttemptContext context, String extension ) throws IOException {
      return outputFile;
    }
  }

  public String generateAlias( String pvfsPath ) {
    return inClassloader( () -> {
        FileSystem fs = FileSystem.get( new URI( pvfsPath ), job.getConfiguration() );
        if ( fs instanceof PvfsHadoopBridgeFileSystemExtension ) {
          return ( ( PvfsHadoopBridgeFileSystemExtension ) fs ).generateAlias( pvfsPath );
        } else {
          return null;
        }
      }
    );
  }
}
