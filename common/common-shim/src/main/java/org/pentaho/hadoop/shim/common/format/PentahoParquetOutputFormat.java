/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2017 by Pentaho : http://www.pentaho.com
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
//#endif
//#if shim_type=="CDH" || shim_type=="MAPR"
//$import parquet.column.ParquetProperties;
//$import parquet.hadoop.ParquetOutputFormat;
//$import parquet.hadoop.ParquetRecordWriter;
//#endif

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.hadoop.shim.api.format.PentahoOutputFormat;
import org.pentaho.hadoop.shim.api.format.PentahoRecordWriter;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;
import org.pentaho.hadoop.shim.common.ConfigurationProxy;

/**
 * Created by Vasilina_Terehova on 8/3/2017.
 */
public class PentahoParquetOutputFormat implements PentahoOutputFormat {

  private static final Logger logger = Logger.getLogger( PentahoParquetInputFormat.class );

  private final Job job;
  private Path outputFile;
  private SchemaDescription schema;

  public PentahoParquetOutputFormat() {
    logger.info( "We are initializing parquet output format" );

    ConfigurationProxy conf;
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
      conf = new ConfigurationProxy();
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }

    try {
      job = Job.getInstance( conf );
    } catch ( IOException ex ) {
      throw new RuntimeException( ex );
    }

    job.getConfiguration().set( ParquetOutputFormat.ENABLE_JOB_SUMMARY, "false" );
    ParquetOutputFormat.setEnableDictionary( job, false );
  }

  @Override
  public void setSchema( SchemaDescription schema ) {
    this.schema = schema;
  }

  @Override
  public void setOutputFile( String file ) {
    outputFile = new Path( file );
    ParquetOutputFormat.setOutputPath( job, outputFile.getParent() );
  }

  @Override
  public void setVersion( VERSION version ) {
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
  }

  @Override
  public void setEncoding( ENCODING encoding ) {
    // TODO implement encoding change
  }

  @Override
  public void setRowGroupSize( int size ) {
    ParquetOutputFormat.setBlockSize( job, size );
  }

  @Override
  public void setDataPageSize( int size ) {
    ParquetOutputFormat.setPageSize( job, size );
  }

  @Override
  public void setDictionaryPageSize( int size ) {
    ParquetOutputFormat.setDictionaryPageSize( job, size );
  }

  @Override
  public PentahoRecordWriter createRecordWriter() {
    if ( outputFile == null ) {
      throw new RuntimeException( "Output file is not defined" );
    }
    if ( schema == null ) {
      throw new RuntimeException( "Schema is not defined" );
    }

    FixedParquetOutputFormat nativeParquetOutputFormat =
        new FixedParquetOutputFormat( new PentahoParquetWriteSupport( schema ) );

    TaskAttemptID taskAttemptID = new TaskAttemptID( "qq", 111, TaskType.MAP, 11, 11 );
    TaskAttemptContextImpl task = new TaskAttemptContextImpl( job.getConfiguration(), taskAttemptID );
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );

      ParquetRecordWriter<RowMetaAndData> recordWriter =
          (ParquetRecordWriter<RowMetaAndData>) nativeParquetOutputFormat.getRecordWriter( task );
      return new PentahoParquetRecordWriter( recordWriter, task );
    } catch ( IOException e ) {
      throw new RuntimeException( "Some error accessing parquet files", e );
    } catch ( InterruptedException e ) {
      // logging here
      e.printStackTrace();
      throw new RuntimeException( "This should never happen " + e );
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
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
