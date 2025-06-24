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

package org.pentaho.hadoop.shim.common.format.parquet;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetRecordWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.util.Assert;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.format.IParquetInputField;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoParquetInputFormat;
import org.pentaho.hadoop.shim.api.format.ParquetSpec;
import org.pentaho.hadoop.shim.common.ConfigurationProxy;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoApacheInputFormat;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.PentahoTwitterInputFormat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith( Parameterized.class )
public class PentahoParquetRecordWriterTest {

  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList( new Object[][] { { "APACHE", "DATA" }, { "TWITTER", "NULL" } } );
  }

  @Parameterized.Parameter
  public String provider;

  @Parameterized.Parameter( 1 )
  public String testType;

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

    switch ( provider ) {
      case "APACHE":
        org.apache.parquet.hadoop.ParquetOutputFormat.setOutputPath( job, outputFile.getParent() );
        break;
      case "TWITTER":
        ParquetOutputFormat.setOutputPath( job, outputFile.getParent() );
        break;
      default:
        org.junit.Assert.fail( "Invalid provider name used." );
    }

    TaskAttemptID taskAttemptID = new TaskAttemptID( "qq", 111, TaskType.MAP, 11, 11 );
    task = new TaskAttemptContextImpl( job.getConfiguration(), taskAttemptID );
  }

  @Test
  public void recordWriterCreateFile() throws Exception {

    IPentahoOutputFormat.IPentahoRecordWriter writer = null;
    Object recordWriterObject = null;

    switch ( provider ) {
      case "APACHE":
        org.apache.parquet.hadoop.api.WriteSupport apacheSupport =
          new org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoParquetWriteSupport(
            ParquetUtils.createOutputFields( ParquetSpec.DataType.INT_64 ) );
        org.apache.parquet.hadoop.ParquetOutputFormat apacheNativeParquetOutputFormat =
          new org.apache.parquet.hadoop.ParquetOutputFormat<>( apacheSupport );
        org.apache.parquet.hadoop.ParquetRecordWriter<RowMetaAndData> apacheRecordWriter =
          (org.apache.parquet.hadoop.ParquetRecordWriter<RowMetaAndData>) apacheNativeParquetOutputFormat
            .getRecordWriter( task );
        recordWriterObject = apacheRecordWriter;
        writer = new org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoParquetRecordWriter(
          apacheRecordWriter, task );
        break;
      case "TWITTER":
        WriteSupport twitterSupport =
          new org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.PentahoParquetWriteSupport(
            ParquetUtils.createOutputFields( ParquetSpec.DataType.INT_64 ) );
        ParquetOutputFormat twitterNativeParquetOutputFormat =
          new ParquetOutputFormat<>( twitterSupport );
        ParquetRecordWriter<RowMetaAndData> twitterRecordWriter =
          (ParquetRecordWriter<RowMetaAndData>) twitterNativeParquetOutputFormat.getRecordWriter( task );
        recordWriterObject = twitterRecordWriter;
        writer = new org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.PentahoParquetRecordWriter(
          twitterRecordWriter, task );
        break;
      default:
        org.junit.Assert.fail( "Invalid provider name used." );
    }

    RowMetaAndData row = new RowMetaAndData();
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "Name" ) );
    rowMeta.addValueMeta( new ValueMetaString( "Age" ) );
    row.setRowMeta( rowMeta );

    switch ( testType ) {
      case "DATA":
        row.setData( new Object[] { "Alex", "87" } );
        break;
      case "NULL":
        row.setData( new Object[] { null, null } );
        break;
      default:
        org.junit.Assert.fail( "Invalid test type used." );
    }

    writer.write( row );

    switch ( provider ) {
      case "APACHE":
        ( (org.apache.parquet.hadoop.ParquetRecordWriter<RowMetaAndData>) recordWriterObject ).close( task );
        break;
      case "TWITTER":
        ( (ParquetRecordWriter<RowMetaAndData>) recordWriterObject ).close( task );
        break;
      default:
        org.junit.Assert.fail( "Invalid provider name used." );
    }

    Files.walk( Paths.get( tempFile.toString() ) )
      .filter( Files::isRegularFile )
      .forEach( ( f ) -> {
        String file = f.toString();
        if ( file.endsWith( "parquet" ) ) {
          try {
            switch ( testType ) {
              case "DATA":
                IPentahoInputFormat.IPentahoRecordReader recordReader =
                  readCreatedParquetFile( Paths.get( file ).toUri().toString() );
                recordReader.forEach(
                  rowMetaAndData -> Assert.assertTrue( rowMetaAndData.size() == 2 ) );
                break;
              case "NULL":
                Assert.assertTrue( Files.size( Paths.get( file ) ) > 0 );
                break;
              default:
                org.junit.Assert.fail( "Invalid test type used." );
            }
          } catch ( Exception e ) {
            e.printStackTrace();
          }
        }
      } );
  }

  private IPentahoInputFormat.IPentahoRecordReader readCreatedParquetFile( String parquetFilePath ) throws Exception {

    IPentahoParquetInputFormat pentahoParquetInputFormat = null;
    PentahoInputSplitImpl pentahoInputSplit = null;
    IPentahoInputFormat.IPentahoRecordReader recordReader = null;

    switch ( provider ) {
      case "APACHE":
        pentahoParquetInputFormat = new PentahoApacheInputFormat( mock( NamedCluster.class ) );

        org.apache.hadoop.mapreduce.lib.input.FileSplit apacheFileSplit = Mockito.mock(org.apache.hadoop.mapreduce.lib.input.FileSplit.class);
        when( apacheFileSplit.getPath() ).thenReturn( new org.apache.hadoop.fs.Path( parquetFilePath ) );
        pentahoInputSplit = new PentahoInputSplitImpl( apacheFileSplit );

        break;
      case "TWITTER":
        pentahoParquetInputFormat = new PentahoTwitterInputFormat( mock( NamedCluster.class ) );

        org.apache.hadoop.mapreduce.lib.input.FileSplit twitterFileSplit = Mockito.mock(org.apache.hadoop.mapreduce.lib.input.FileSplit.class);
        when( twitterFileSplit.getPath() ).thenReturn( new org.apache.hadoop.fs.Path( parquetFilePath ) );
        pentahoInputSplit = new PentahoInputSplitImpl( twitterFileSplit );

        break;
      default:
        org.junit.Assert.fail( "Invalid provider name used." );
    }

    pentahoParquetInputFormat.setInputFile( parquetFilePath );
    List<IParquetInputField> schema =
      (List<IParquetInputField>) pentahoParquetInputFormat.readSchema( parquetFilePath );
    pentahoParquetInputFormat.setSchema( schema );

    recordReader = pentahoParquetInputFormat.createRecordReader( pentahoInputSplit );
    return recordReader;
  }
}
