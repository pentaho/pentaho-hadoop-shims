/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Assert;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;
import org.pentaho.hadoop.shim.common.ConfigurationProxy;

import java.util.Arrays;


@RunWith( Parameterized.class )
public class PentahoParquetRecordReaderTest {

  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList( new Object[][] { { "APACHE", "DATA", "sample.pqt" }, { "TWITTER", "DATA", "sample.pqt" },
      { "APACHE", "EMPTY", "empty.pqt" }, { "TWITTER", "EMPTY", "empty.pqt" } } );
  }

  @Parameterized.Parameter
  public String provider;

  @Parameterized.Parameter( 1 )
  public String testType;

  @Parameterized.Parameter( 2 )
  public String testFile;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void iterateOverParquetFile() throws Exception {
    ConfigurationProxy conf = new ConfigurationProxy();
    conf.set( "fs.defaultFS", "file:///" );
    Job job = Job.getInstance( conf );
    String marshallStr = null;

    switch ( testType ) {
      case "DATA":
        marshallStr =
          new ParquetInputFieldList( ParquetUtils.createSchema( ValueMetaInterface.TYPE_INTEGER ) ).marshall();
        expectedException = ExpectedException.none();
        break;
      case "EMPTY":
        marshallStr = new SchemaDescription().marshall();
        expectedException.expect( RuntimeException.class );
        break;
      default:
        org.junit.Assert.fail( "Invalid test type used." );
    }

    switch ( provider ) {
      case "APACHE":
        job.getConfiguration()
          .set( org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.ParquetConverter.PARQUET_SCHEMA_CONF_KEY,
            marshallStr );
        org.apache.parquet.hadoop.api.ReadSupport<RowMetaAndData> apacheReadSupport =
          new org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoParquetReadSupport();
        org.apache.parquet.hadoop.ParquetRecordReader<RowMetaAndData> apacheNativeRecordReader =
          new org.apache.parquet.hadoop.ParquetRecordReader<>( apacheReadSupport,
            org.apache.parquet.hadoop.ParquetInputFormat.getFilter( job.getConfiguration() ) );
        org.apache.parquet.hadoop.ParquetInputFormat<RowMetaAndData> apacheNativeParquetInputFormat =
          new org.apache.parquet.hadoop.ParquetInputFormat<>();
        FileInputFormat.setInputPaths( job, getClass().getClassLoader().getResource( testFile ).toExternalForm() );
        InputSplit apacheInputSplit = apacheNativeParquetInputFormat.getSplits( job ).get( 0 );
        TaskAttemptContextImpl apacheTask = new TaskAttemptContextImpl( job.getConfiguration(), new TaskAttemptID() );
        apacheNativeRecordReader.initialize( apacheInputSplit, apacheTask );
        org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoParquetRecordReader apacheRecordReader =
          new org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoParquetRecordReader(
            apacheNativeRecordReader );

        switch ( testType ) {
          case "DATA":
            Assert.assertTrue( apacheRecordReader.iterator().hasNext() );
            Assert.assertNotNull( apacheRecordReader.iterator().next() );
            break;
          case "EMPTY":
            Assert.assertFalse( apacheRecordReader.iterator().hasNext() );
            Assert.assertNull( apacheRecordReader.iterator().next() );
            break;
          default:
            org.junit.Assert.fail( "Invalid test type used." );
        }

        apacheRecordReader.close();
        break;
      case "TWITTER":
        job.getConfiguration()
          .set( org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.ParquetConverter.PARQUET_SCHEMA_CONF_KEY,
            marshallStr );
        parquet.hadoop.api.ReadSupport<RowMetaAndData> twitterReadSupport =
          new org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.PentahoParquetReadSupport();
        parquet.hadoop.ParquetRecordReader<RowMetaAndData> twitterNativeRecordReader =
          new parquet.hadoop.ParquetRecordReader<>( twitterReadSupport,
            parquet.hadoop.ParquetInputFormat.getFilter( job.getConfiguration() ) );
        parquet.hadoop.ParquetInputFormat<RowMetaAndData> twitterNativeParquetInputFormat =
          new parquet.hadoop.ParquetInputFormat<>();
        FileInputFormat.setInputPaths( job, getClass().getClassLoader().getResource( testFile ).toExternalForm() );
        InputSplit twitterInputSplit = twitterNativeParquetInputFormat.getSplits( job ).get( 0 );
        TaskAttemptContextImpl twitterTask = new TaskAttemptContextImpl( job.getConfiguration(), new TaskAttemptID() );
        twitterNativeRecordReader.initialize( twitterInputSplit, twitterTask );
        org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.PentahoParquetRecordReader twitterRecordReader =
          new org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.PentahoParquetRecordReader(
            twitterNativeRecordReader );

        switch ( testType ) {
          case "DATA":
            Assert.assertTrue( twitterRecordReader.iterator().hasNext() );
            Assert.assertNotNull( twitterRecordReader.iterator().next() );
            break;
          case "EMPTY":
            Assert.assertFalse( twitterRecordReader.iterator().hasNext() );
            Assert.assertNull( twitterRecordReader.iterator().next() );
            break;
          default:
            org.junit.Assert.fail( "Invalid test type used." );
        }

        twitterRecordReader.close();
        break;
      default:
        org.junit.Assert.fail( "Invalid provider name used." );
    }
  }
}
