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
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Logger;

//#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI" || shim_type=="MAPR"
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetRecordReader;
//#endif
//#if shim_type=="CDH"
//$import parquet.hadoop.ParquetInputFormat;
//$import parquet.hadoop.ParquetRecordReader;
//#endif

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.hadoop.shim.api.format.PentahoInputFormat;
import org.pentaho.hadoop.shim.api.format.PentahoInputSplit;
import org.pentaho.hadoop.shim.api.format.PentahoRecordReader;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;
import org.pentaho.hadoop.shim.common.ConfigurationProxy;

/**
 * Created by Vasilina_Terehova on 7/25/2017.
 */
public class PentahoParquetInputFormat implements PentahoInputFormat {

  private static final Logger logger = Logger.getLogger( PentahoParquetInputFormat.class );

  private final Configuration conf;
  private final ParquetInputFormat<RowMetaAndData> nativeParquetInputFormat;
  private final JobContextImpl jobContext;
  private final TaskAttemptID taskAttemptID;

  public PentahoParquetInputFormat() {
    logger.info( "We are initializing parquet input format" );

    conf = new ConfigurationProxy();

    JobID jobId = new JobID( "Job name", ParquetConverter.PARQUET_JOB_ID );
    jobContext = new JobContextImpl( conf, jobId );

    taskAttemptID = new TaskAttemptID();
    nativeParquetInputFormat = new ParquetInputFormat<>();

    conf.set( ParquetInputFormat.READ_SUPPORT_CLASS, ParquetConverter.MyParquetReadSupport.class.getName() );
  }

  @Override
  public void setSchema( SchemaDescription schema ) {
    conf.set( ParquetConverter.PARQUET_SCHEMA_CONF_KEY, schema.marshall() );
  }

  @Override
  public void setInputDir( String dir ) {
    conf.set( ParquetInputFormat.INPUT_DIR, dir );
    conf.set( ParquetInputFormat.INPUT_DIR_RECURSIVE, "false" );
  }

  @Override
  public void setSplitSize( long blockSize ) {
    conf.set( ParquetInputFormat.SPLIT_MAXSIZE, Long.toString( blockSize ) );
    conf.set( ParquetInputFormat.TASK_SIDE_METADATA, "false" );
  }

  @Override
  public List<PentahoInputSplit> getSplits() throws IOException {
    List<InputSplit> splits = nativeParquetInputFormat.getSplits( jobContext );
    return splits.stream().map( PentahoInputSplitImpl::new ).collect( Collectors.toList() );
  }

  // for parquet not actual to point split
  @Override
  public PentahoRecordReader createRecordReader( PentahoInputSplit split ) throws IOException, InterruptedException {
    TaskAttemptContextImpl task = new TaskAttemptContextImpl( conf, taskAttemptID );
    PentahoInputSplitImpl pentahoInputSplit = (PentahoInputSplitImpl) split;
    InputSplit inputSplit = pentahoInputSplit.getInputSplit();
    ParquetRecordReader<RowMetaAndData> rd = (ParquetRecordReader<RowMetaAndData>) nativeParquetInputFormat.createRecordReader( inputSplit, task );
    rd.initialize( inputSplit, task );
    return new PentahoParquetRecordReader(  rd );
  }
}
