/*! ******************************************************************************
 *
 * Pentaho Data Integration
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
package org.pentaho.hadoop.shim.common.format.parquet;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
//#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI"
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.apache.parquet.hadoop.api.ReadSupport;
//#endif
//#if shim_type=="CDH" || shim_type=="MAPR"
//$import parquet.hadoop.ParquetInputFormat;
//$import parquet.hadoop.ParquetRecordReader;
//$import parquet.hadoop.api.ReadSupport;
//#endif
import org.junit.Test;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Assert;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;
import org.pentaho.hadoop.shim.common.ConfigurationProxy;
import org.pentaho.hadoop.shim.common.format.ParquetUtils;

public class PentahoParquetRecordReaderTest {

  private Job job;
  private ParquetRecordReader<RowMetaAndData> nativeRecordReader;
  private ParquetInputFormat<RowMetaAndData> nativeParquetInputFormat;

  public void initSample() throws Exception {
    ConfigurationProxy conf = new ConfigurationProxy();
    conf.set( "fs.defaultFS", "file:///" );
    job = Job.getInstance( conf );
    SchemaDescription schema = ParquetUtils.createSchema( ValueMetaInterface.TYPE_INTEGER );
    job.getConfiguration().set( ParquetConverter.PARQUET_SCHEMA_CONF_KEY, schema.marshall() );
    ReadSupport<RowMetaAndData> readSupport = new PentahoParquetReadSupport();
    nativeRecordReader =
        new ParquetRecordReader<>( readSupport, ParquetInputFormat.getFilter( job.getConfiguration() ) );
    nativeParquetInputFormat = new ParquetInputFormat<>();
  }

  public void initEmpty() throws Exception {
    ConfigurationProxy conf = new ConfigurationProxy();
    conf.set( "fs.defaultFS", "file:///" );
    job = Job.getInstance( conf );
    SchemaDescription schema = new SchemaDescription();
    job.getConfiguration().set( ParquetConverter.PARQUET_SCHEMA_CONF_KEY, schema.marshall() );
    ReadSupport<RowMetaAndData> readSupport = new PentahoParquetReadSupport();
    nativeRecordReader =
        new ParquetRecordReader<>( readSupport, ParquetInputFormat.getFilter( job.getConfiguration() ) );
    nativeParquetInputFormat = new ParquetInputFormat<>();
  }

  @Test
  public void iterateOverParquetFileWithData() throws Exception {
    initSample();
    FileInputFormat.setInputPaths( job, getClass().getClassLoader().getResource( "sample.pqt" ).toExternalForm() );
    initializeRecordReader();
    PentahoParquetRecordReader recordReader = new PentahoParquetRecordReader( nativeRecordReader );

    Assert.assertTrue( recordReader.iterator().hasNext() );
    Assert.assertNotNull( recordReader.iterator().next() );

    recordReader.close();
  }

  @Test( expected = RuntimeException.class )
  public void iterateOverEmptyParquetFile() throws Exception {
    initEmpty();
    FileInputFormat.setInputPaths( job, getClass().getClassLoader().getResource( "empty.pqt" ).toExternalForm() );
    initializeRecordReader();
    PentahoParquetRecordReader recordReader = new PentahoParquetRecordReader( nativeRecordReader );

    Assert.assertFalse( recordReader.iterator().hasNext() );
    Assert.assertNull( recordReader.iterator().next() );

    recordReader.close();
  }

  private void initializeRecordReader() throws Exception {
    InputSplit inputSplit = nativeParquetInputFormat.getSplits( job ).get( 0 );
    TaskAttemptContextImpl task = new TaskAttemptContextImpl( job.getConfiguration(), new TaskAttemptID() );
    nativeRecordReader.initialize( inputSplit, task );
  }
}
