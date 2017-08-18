/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Logger;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.format.PentahoInputFormat;
import org.pentaho.hadoop.shim.api.format.PentahoInputSplit;
import org.pentaho.hadoop.shim.api.format.RecordReader;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;
import org.pentaho.hadoop.shim.api.fs.FileSystem;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

//#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI" || shim_type=="MAPR"
//#endif
//#if shim_type=="CDH"
//$import parquet.hadoop.ParquetInputFormat;
//$import parquet.hadoop.ParquetRecordReader;
//#endif

/**
 * Created by Vasilina_Terehova on 7/25/2017.
 */
public class PentahoParquetInputFormat implements PentahoInputFormat {
  public static long SPLIT_SIZE = 128 * 1024 * 1024;

  public static final int JOB_ID = Integer.MAX_VALUE;
  private Configuration conf;
  private ParquetInputFormat<String> nativeParquetInputFormat;
  private JobContextImpl jobContext;
  private JobID jobId;
  private TaskAttemptID taskAttemptID;
  private Logger logger = Logger.getLogger( getClass() );

  public PentahoParquetInputFormat( Configuration jobConfiguration, SchemaDescription schema, FileSystem path ) {
    logger.error( "We are initializing parquet input format" );

    // make builder for configuration to set base params
    jobConfiguration.set( ParquetInputFormat.SPLIT_MAXSIZE, Long.toString( SPLIT_SIZE ) );
    jobConfiguration.set( ParquetInputFormat.TASK_SIDE_METADATA, "false" );
    jobConfiguration.set( ParquetInputFormat.READ_SUPPORT_CLASS, ParquetConverter.MyParquetReadSupport.class
      .getName() );
    jobConfiguration.set( "PentahoParquetSchema", schema.marshall() );

    this.conf = jobConfiguration;
    org.apache.hadoop.conf.Configuration asDelegateConf =
      conf.getAsDelegateConf( org.apache.hadoop.conf.Configuration.class );

    jobId = new JobID( "Job name", JOB_ID );
    jobContext = new JobContextImpl( asDelegateConf, jobId );
    taskAttemptID = new TaskAttemptID();
    nativeParquetInputFormat = new ParquetInputFormat<>();
  }

  @Override
  public List<PentahoInputSplit> getSplits() throws IOException {
    List<InputSplit> splits = nativeParquetInputFormat.getSplits( jobContext );
    return splits.stream().map( PentahoInputSplitImpl::new ).collect( Collectors.toList() );
  }

  // for parquet not actual to point split
  @Override
  public RecordReader getRecordReader( PentahoInputSplit split ) throws IOException, InterruptedException {
    org.apache.hadoop.conf.Configuration asDelegateConf =
      conf.getAsDelegateConf( org.apache.hadoop.conf.Configuration.class );
    TaskAttemptContextImpl task = new TaskAttemptContextImpl( asDelegateConf, taskAttemptID );
    PentahoInputSplitImpl pentahoInputSplit = (PentahoInputSplitImpl) split;
    InputSplit inputSplit = pentahoInputSplit.getInputSplit();
    ParquetRecordReader rd = (ParquetRecordReader) nativeParquetInputFormat.createRecordReader( inputSplit, task );
    rd.initialize( inputSplit, task );
    return new PentahoParquetRecordReader( nativeParquetInputFormat, rd, jobContext );
  }

  @Override public Configuration getActiveConfiguration() {
    return conf;
  }
}
