/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2017 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.common.format;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Logger;

//#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI"
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetRecordWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
//#endif
//#if shim_type=="CDH" || shim_type=="MAPR"
//$import parquet.column.ParquetProperties;
//$import parquet.hadoop.ParquetOutputFormat;
//$import parquet.hadoop.ParquetRecordWriter;
//$import parquet.hadoop.metadata.CompressionCodecName;
//#endif

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.hadoop.shim.api.format.IPentahoParquetOutputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;
import org.pentaho.hadoop.shim.common.ConfigurationProxy;
import org.pentaho.hadoop.shim.common.format.parquet.PentahoParquetRecordWriter;
import org.pentaho.hadoop.shim.common.format.parquet.PentahoParquetWriteSupport;

/**
 * Created by Vasilina_Terehova on 8/3/2017.
 */
public class PentahoParquetOutputFormat extends HadoopFormatBase implements IPentahoParquetOutputFormat {

  private static final Logger logger = Logger.getLogger( PentahoParquetInputFormat.class );

  private Job job;
  private Path outputFile;
  private SchemaDescription schema;

  public PentahoParquetOutputFormat() throws Exception {
    logger.info( "We are initializing parquet output format" );

    inClassloader( () -> {
      ConfigurationProxy conf = new ConfigurationProxy();

      job = Job.getInstance( conf );

      job.getConfiguration().set( ParquetOutputFormat.ENABLE_JOB_SUMMARY, "false" );
      ParquetOutputFormat.setEnableDictionary( job, false );
    } );
  }

  @Override
  public void setSchema( SchemaDescription schema ) {
    this.schema = schema;
  }

  @Override
  public void setOutputFile( String file, boolean override ) throws Exception {
    inClassloader( () -> {
      outputFile = new Path( file );
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
    inClassloader( () -> {
      ParquetOutputFormat.setEnableDictionary( job, useDictionary );
    } );
  }

  @Override
  public void setRowGroupSize( int size ) throws Exception {
    inClassloader( () -> {
      ParquetOutputFormat.setBlockSize( job, size );
    } );
  }

  @Override
  public void setDataPageSize( int size ) throws Exception {
    inClassloader( () -> {
      ParquetOutputFormat.setPageSize( job, size );
    } );
  }

  @Override
  public void setDictionaryPageSize( int size ) throws Exception {
    inClassloader( () -> {
      ParquetOutputFormat.setDictionaryPageSize( job, size );
    } );
  }

  @Override
  public IPentahoRecordWriter createRecordWriter() throws Exception {
    if ( outputFile == null ) {
      throw new RuntimeException( "Output file is not defined" );
    }
    if ( schema == null ) {
      throw new RuntimeException( "Schema is not defined" );
    }

    return inClassloader( () -> {
      FixedParquetOutputFormat nativeParquetOutputFormat =
          new FixedParquetOutputFormat( new PentahoParquetWriteSupport( schema ) );

      TaskAttemptID taskAttemptID = new TaskAttemptID( "qq", 111, TaskType.MAP, 11, 11 );
      TaskAttemptContextImpl task = new TaskAttemptContextImpl( job.getConfiguration(), taskAttemptID );
      try {

        ParquetRecordWriter<RowMetaAndData> recordWriter =
            (ParquetRecordWriter<RowMetaAndData>) nativeParquetOutputFormat.getRecordWriter( task );
        return new PentahoParquetRecordWriter( recordWriter, task );
      } catch ( IOException e ) {
        throw new RuntimeException( "Some error accessing parquet files", e );
      } catch ( InterruptedException e ) {
        // logging here
        e.printStackTrace();
        throw new RuntimeException( "This should never happen " + e );
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
}
