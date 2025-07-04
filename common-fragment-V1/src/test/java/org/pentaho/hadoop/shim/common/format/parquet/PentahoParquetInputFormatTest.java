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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBigNumber;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.row.value.ValueMetaTimestamp;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.format.IParquetInputField;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat.IPentahoInputSplit;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat.IPentahoRecordReader;
import org.pentaho.hadoop.shim.api.format.IPentahoParquetInputFormat;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoApacheInputFormat;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.PentahoTwitterInputFormat;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.NoSuchFileException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith( Parameterized.class )
public class PentahoParquetInputFormatTest {

  private static LogChannelInterface log = new LogChannel( PentahoParquetInputFormatTest.class.getName() );

  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList( new Object[][] { { "APACHE" }, { "TWITTER" } } );
  }

  @Parameterized.Parameter
  public String provider;

  private IPentahoParquetInputFormat pentahoParquetInputFormat;
  private String parquetFilePath = getClass().getClassLoader().getResource( "sample.pqt" ).toExternalForm();

  @Before
  public void resetInputFormatBeforeEachTest() throws Exception {
    KettleLogStore.init();
    NamedCluster namedCluster = mock( NamedCluster.class );
    KettleLogStore.init();
    switch ( provider ) {
      case "APACHE":
        pentahoParquetInputFormat = new PentahoApacheInputFormat( namedCluster );
        break;
      case "TWITTER":
        pentahoParquetInputFormat = new PentahoTwitterInputFormat( namedCluster );
        break;
      default:
        fail( "Invalid provider name used." );
    }
  }

  @Test
  public void createRecordReader() throws Exception {

    pentahoParquetInputFormat.setInputFile( getClass().getClassLoader().getResource( "sample.pqt" ).toExternalForm() );
    List<IParquetInputField> schema =
      (List<IParquetInputField>) pentahoParquetInputFormat.readSchema( parquetFilePath );
    pentahoParquetInputFormat.setSchema( schema );

    PentahoInputSplitImpl pentahoInputSplit = null;
    switch ( provider ) {
      case "APACHE":
        FileSplit apacheFileSplit = Mockito.mock(FileSplit.class);
        when(apacheFileSplit.getPath()).thenReturn(new Path(parquetFilePath));
        pentahoInputSplit = new PentahoInputSplitImpl(apacheFileSplit);
        break;
      case "TWITTER":
        FileSplit twitterFileSplit = Mockito.mock(FileSplit.class);
        when(twitterFileSplit.getPath()).thenReturn(new Path(parquetFilePath));
        pentahoInputSplit = new PentahoInputSplitImpl(twitterFileSplit);
        break;
      default:
        fail( "Invalid provider name used." );
    }

    IPentahoRecordReader recordReader =
      pentahoParquetInputFormat.createRecordReader( pentahoInputSplit );

    Assert.assertNotNull( "recordReader should NOT be null!", recordReader );
    Assert.assertTrue( "recordReader should be instance of IPentahoInputFormat.IPentahoRecordReader",
      recordReader instanceof IPentahoRecordReader );
  }

  @Test
  public void testFileInput() throws Exception {
    readData( "parquet/1_uncompressed_nodict.par" );
    readData( "parquet/1_gzip_nodict.par" );
    // readData( "parquet/1_lzo_nodict.par");
    readData( "parquet/1_snappy_nodict.par" );
    readData( "parquet/1_uncompressed_dict.par" );

    readData( "parquet/2_uncompressed_nodict.par" );
    readData( "parquet/2_gzip_nodict.par" );
    // readData( "parquet/2_lzo_nodict.par");
    readData( "parquet/2_snappy_nodict.par" );
    readData( "parquet/2_uncompressed_dict.par" );

    readData( "parquet/1_spark.par" );
  }

  @Test
  public void testSpacesInFilePath() throws Exception {
    Exception exception = null;
    try {
      pentahoParquetInputFormat.setInputFile( "/test test/out.txt" );
      fail( "Expected exception.  No such file." );
    } catch ( Exception e ) {
      exception = e;
    }
    //BACKLOG-19435: NoSuchFileException or IOException (mapr) is expected after this change not URISyntaxException
    Assert
      .assertTrue( exception.getCause() instanceof NoSuchFileException
        || exception.getCause() instanceof IOException );
  }

  private void readData( String file ) throws Exception {
    RowMeta expectedRowMeta = new RowMeta();
    expectedRowMeta.addValueMeta( new ValueMetaNumber( "fnum" ) );
    expectedRowMeta.addValueMeta( new ValueMetaString( "fstring" ) );
    expectedRowMeta.addValueMeta( new ValueMetaTimestamp( "fdate" ) );
    expectedRowMeta.addValueMeta( new ValueMetaBoolean( "fbool" ) );
    expectedRowMeta.addValueMeta( new ValueMetaInteger( "fint" ) );
    expectedRowMeta.addValueMeta( new ValueMetaBigNumber( "fbignum" ) );
    expectedRowMeta.addValueMeta( new ValueMetaTimestamp( "ftime" ) );

    SimpleDateFormat df = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
    df.setTimeZone( TimeZone.getTimeZone( "Europe/Minsk" ) );

    List<Object[]> expectedData = new ArrayList<>();
    expectedData.add( new Object[] {
      2.1, null, 2.1, 2.1 } );
    expectedData.add( new Object[] {
      "John", "Paul", "George", "Ringo" } );
    expectedData.add( new Object[] {
      df.parse( "2018-01-01 13:00:00" ), df.parse( "2018-01-01 09:10:15" ), null, df.parse( "2018-01-01 09:10:35" ) } );
    expectedData.add( new Object[] {
      true, false, true, null } );
    expectedData.add( new Object[] {
      1L, 3L, null, 4L } );
    expectedData.add( new Object[] {
      new BigDecimal( 4.5 ), null, new BigDecimal( 4.5 ), new BigDecimal( 4.5 ) } );
    expectedData.add( new Object[] {
      null, new Timestamp( df.parse( "2018-05-01 13:00:00" ).getTime() ),
      new Timestamp( df.parse( "2018-05-01 13:00:00" ).getTime() ),
      new Timestamp( df.parse( "2018-05-01 13:00:00" ).getTime() ) } );

    List<? extends IParquetInputField> fileFields =
      pentahoParquetInputFormat.readSchema( getClass().getClassLoader().getResource( file ).toExternalForm() );

    // fix after autodetection
    Assert.assertEquals( ValueMetaInterface.TYPE_NUMBER, fileFields.get( 5 ).getPentahoType() );
    fileFields.get( 5 ).setPentahoType( ValueMetaInterface.TYPE_BIGNUMBER );
    Assert.assertEquals( ValueMetaInterface.TYPE_TIMESTAMP, fileFields.get( 6 ).getPentahoType() );

    // check fields from file
    Assert.assertEquals( expectedRowMeta.size(), fileFields.size() );
    for ( int i = 0; i < expectedRowMeta.size(); i++ ) {
      ValueMetaInterface vmExpected = expectedRowMeta.getValueMeta( i );
      Assert.assertEquals( vmExpected.getName(), fileFields.get( i ).getFormatFieldName() );
      Assert.assertEquals( vmExpected.getType(), fileFields.get( i ).getPentahoType() );
    }

    // check read by one field
    for ( int i = 0; i < fileFields.size(); i++ ) {
      List<ValueMetaInterface> expectedFields = new ArrayList<>();
      List<Object[]> expectedRows = new ArrayList<>();
      List<IParquetInputField> readSchema = new ArrayList<>();

      expectedFields.add( expectedRowMeta.getValueMeta( i ) );
      expectedRows.add( expectedData.get( i ) );
      readSchema.add( fileFields.get( i ) );

      List<RowMetaAndData> rows = readFile( file, readSchema );
      checkRows( expectedFields, expectedRows, rows );
    }
    // check read in random fields order - 3 times
    Random random = new Random();
    for ( int r = 0; r < 5; r++ ) {
      List<ValueMetaInterface> expectedFields = new ArrayList<>();
      List<Object[]> expectedRows = new ArrayList<>();
      List<IParquetInputField> readSchema = new ArrayList<>();

      int count = expectedRowMeta.getValueMetaList().size();
      Set<Integer> used = new TreeSet<>();
      for ( int i = 0; i < count; i++ ) {
        int rv = random.nextInt( count );
        if ( used.add( rv ) ) {
          expectedFields.add( expectedRowMeta.getValueMeta( rv ) );
          expectedRows.add( expectedData.get( rv ) );
          readSchema.add( fileFields.get( rv ) );
        }
      }

      List<RowMetaAndData> rows = readFile( file, readSchema );
      checkRows( expectedFields, expectedRows, rows );
    }
  }

  private List<RowMetaAndData> readFile( String file, List<IParquetInputField> readSchema ) throws Exception {
    log.logBasic( "Read '" + file + "' as schema: " + readSchema );
    pentahoParquetInputFormat.setInputFile( getClass().getClassLoader().getResource( file ).toExternalForm() );
    pentahoParquetInputFormat.setSchema( readSchema );

    List<RowMetaAndData> rows = new ArrayList<>();
    for ( IPentahoInputSplit split : pentahoParquetInputFormat.getSplits() ) {
      IPentahoRecordReader rd = pentahoParquetInputFormat.createRecordReader( split );
      rd.forEach( row -> rows.add( row ) );
      rd.close();
    }

    return rows;
  }

  private void checkRows( List<ValueMetaInterface> expectedColumns, List<Object[]> expectedRows,
                          List<RowMetaAndData> rows ) {
    Assert.assertEquals( 4, rows.size() );
    for ( int r = 0; r < rows.size(); r++ ) {
      RowMetaAndData row = rows.get( r );
      Assert.assertEquals( expectedColumns.size(), row.getRowMeta().getValueMetaList().size() );
      for ( int i = 0; i < expectedColumns.size(); i++ ) {
        ValueMetaInterface vmExpected = expectedColumns.get( i );
        ValueMetaInterface exist = row.getRowMeta().getValueMeta( i );
        // check field
        Assert.assertEquals( vmExpected.getName(), exist.getName() );
        Assert.assertEquals( vmExpected.getType(), exist.getType() );
        // check value
        Assert.assertEquals( expectedRows.get( i )[ r ], row.getData()[ i ] );
      }
    }
  }
}
