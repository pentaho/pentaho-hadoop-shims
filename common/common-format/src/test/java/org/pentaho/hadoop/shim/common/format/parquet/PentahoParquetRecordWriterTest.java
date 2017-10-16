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

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
//#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI"
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetRecordWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
//#endif
//#if shim_type=="CDH" || shim_type=="MAPR"
//$import parquet.hadoop.ParquetInputSplit;
//$import parquet.hadoop.ParquetOutputFormat;
//$import parquet.hadoop.ParquetRecordWriter;
//$import parquet.hadoop.api.WriteSupport;
//#endif
//#if shim_type=="MAPR"
//$import org.junit.Assume;
//$import org.junit.BeforeClass;
//$import org.pentaho.di.core.Const;
//#endif
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.util.Assert;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;
import org.pentaho.hadoop.shim.common.ConfigurationProxy;
import org.pentaho.hadoop.shim.common.format.ParquetUtils;
import org.pentaho.hadoop.shim.common.format.PentahoParquetInputFormat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


public class PentahoParquetRecordWriterTest {

  //#if shim_type=="MAPR"
  //$@BeforeClass
  //$public static void setUpBeforeClass() {
  //$  Assume.assumeTrue( Const.isLinux() );
  //$}
  //#endif

  private static Path tempFile = null;
  private static final String PARQUET_FILE_NAME = "/test.parquet";
  private static TaskAttemptContextImpl task = null;

  @Before
  public void setUp() throws Exception {

    ConfigurationProxy conf = new ConfigurationProxy();
    conf.set( "fs.defaultFS", "file:///" );
    Job job = Job.getInstance( conf );

    tempFile = Files.createTempDirectory( "parquet" );

    org.apache.hadoop.fs.Path outputFile = new org.apache.hadoop.fs.Path( tempFile + PARQUET_FILE_NAME );

    ParquetOutputFormat.setOutputPath( job, outputFile.getParent() );

    TaskAttemptID taskAttemptID = new TaskAttemptID( "qq", 111, TaskType.MAP, 11, 11 );

    task = new TaskAttemptContextImpl( job.getConfiguration(), taskAttemptID );
  }

  @Test
  public void recordWriterCreateFileWithData() throws Exception {

    WriteSupport support =
      new PentahoParquetWriteSupport( ParquetUtils.createSchema( ValueMetaInterface.TYPE_INTEGER ) );

    ParquetOutputFormat nativeParquetOutputFormat = new ParquetOutputFormat<>( support );

    ParquetRecordWriter<RowMetaAndData> recordWriter =
      (ParquetRecordWriter<RowMetaAndData>) nativeParquetOutputFormat.getRecordWriter( task );

    PentahoParquetRecordWriter writer = new PentahoParquetRecordWriter( recordWriter, task );

    RowMetaAndData
      row = new RowMetaAndData();
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "Name" ) );
    rowMeta.addValueMeta( new ValueMetaString( "Age" ) );
    row.setRowMeta( rowMeta );
    row.setData( new Object[] { "Alex", "87" } );

    writer.write( row );
    recordWriter.close( task );

    Files.walk( Paths.get( tempFile.toString() ) )
      .filter( Files::isRegularFile )
      .forEach( ( f ) -> {
        String file = f.toString();
        if ( file.endsWith( "parquet" ) ) {
          IPentahoInputFormat.IPentahoRecordReader recordReader =
            readCreatedParquetFile( Paths.get( file ).toUri().toString() );
          recordReader.forEach(
            rowMetaAndData -> Assert.assertTrue( rowMetaAndData.size() == 2 ) );
        }
      } );
  }

  @Test
  public void recordWriterCreateFileWithoutData() throws Exception {

    WriteSupport support =
      new PentahoParquetWriteSupport( ParquetUtils.createSchema( ValueMetaInterface.TYPE_INTEGER ) );

    ParquetOutputFormat nativeParquetOutputFormat = new ParquetOutputFormat<>( support );

    ParquetRecordWriter<RowMetaAndData> recordWriter =
      (ParquetRecordWriter<RowMetaAndData>) nativeParquetOutputFormat.getRecordWriter( task );

    PentahoParquetRecordWriter writer = new PentahoParquetRecordWriter( recordWriter, task );

    RowMetaAndData
      row = new RowMetaAndData();
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "Name" ) );
    rowMeta.addValueMeta( new ValueMetaString( "Age" ) );
    row.setRowMeta( rowMeta );
    row.setData( new Object[] { null, null } );

    writer.write( row );
    recordWriter.close( task );

    Files.walk( Paths.get( tempFile.toString() ) )
      .filter( Files::isRegularFile )
      .forEach( ( f ) -> {
        String file = f.toString();
        if ( file.endsWith( "parquet" ) ) {
          try {
            Assert.assertTrue( Files.size( Paths.get( file ) ) > 0 );
          } catch ( IOException e ) {
            e.printStackTrace();
          }
        }
      } );
  }

  private IPentahoInputFormat.IPentahoRecordReader readCreatedParquetFile( String parquetFilePath ) {

    IPentahoInputFormat.IPentahoRecordReader recordReader = null;
    try {
      PentahoParquetInputFormat pentahoParquetInputFormat = new PentahoParquetInputFormat();
      pentahoParquetInputFormat.setInputFile( parquetFilePath );
      SchemaDescription schema = pentahoParquetInputFormat.readSchema( parquetFilePath );
      pentahoParquetInputFormat.setSchema( schema );

      ParquetInputSplit parquetInputSplit = Mockito.spy( ParquetInputSplit.class );
      Whitebox.setInternalState( parquetInputSplit, "rowGroupOffsets", new long[] { 4 } );
      Whitebox.setInternalState( parquetInputSplit, "file", new org.apache.hadoop.fs.Path( parquetFilePath ) );
      PentahoInputSplitImpl pentahoInputSplit = new PentahoInputSplitImpl( parquetInputSplit );

      recordReader = pentahoParquetInputFormat.createRecordReader( pentahoInputSplit );
    } catch ( Exception e ) {
      e.printStackTrace();
    }
    return recordReader;
  }
}
