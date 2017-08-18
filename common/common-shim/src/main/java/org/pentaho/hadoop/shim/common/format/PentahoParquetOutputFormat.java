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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Logger;

//#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI" || shim_type=="MAPR"
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetRecordWriter;
//#endif
//#if shim_type=="CDH"
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

  private final Configuration conf;
  private final ParquetOutputFormat<RowMetaAndData> nativeParquetOutputFormat;
  private final JobContextImpl jobContext;
  private final TaskAttemptID taskAttemptID;

  public PentahoParquetOutputFormat() {
    logger.info( "We are initializing parquet output format" );

    conf = new ConfigurationProxy();

    JobID jobId = new JobID( "Job name", ParquetConverter.PARQUET_JOB_ID );
    jobContext = new JobContextImpl( conf, jobId );

    taskAttemptID = new TaskAttemptID("qq", 111, TaskType.MAP, 11, 11);
    nativeParquetOutputFormat = new ParquetOutputFormat<>();

    conf.set( ParquetOutputFormat.WRITE_SUPPORT_CLASS, ParquetConverter.MyParquetWriteSupport.class.getName() );
    conf.set( ParquetOutputFormat.ENABLE_JOB_SUMMARY, "false" );
  }

  @Override
  public void setSchema( SchemaDescription schema ) {
    conf.set( ParquetConverter.PARQUET_SCHEMA_CONF_KEY, schema.marshall() );
  }

  @Override
  public void setOutputDir( String dir ) {
    conf.set( ParquetOutputFormat.OUTDIR, dir );
  }

  @Override
  public void setVersion( VERSION ver ) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setEncoding( ENCODING enc ) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setRowGroupSize( long size ) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setDataPageSize( long size ) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setDictionaryPageSize( long size ) {
    // TODO Auto-generated method stub

  }

  @Override
  public PentahoRecordWriter createRecordWriter() {
    TaskAttemptContextImpl task = new TaskAttemptContextImpl( conf, taskAttemptID );
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );

      ParquetRecordWriter<RowMetaAndData> recordWriter =
          (ParquetRecordWriter<RowMetaAndData>) nativeParquetOutputFormat.getRecordWriter( task );
      return new PentahoParquetRecordWriter( recordWriter, task );
    } catch ( IOException e ) {
      throw new RuntimeException( "some eror accessing parquet files", e );
    } catch ( InterruptedException e ) {
      // logging here
      e.printStackTrace();
      throw new RuntimeException( "This should never happen " + e );
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }
}
