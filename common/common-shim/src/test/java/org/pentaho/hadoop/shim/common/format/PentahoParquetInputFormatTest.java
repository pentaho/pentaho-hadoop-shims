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
package org.pentaho.hadoop.shim.common.format;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
//#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI"
import org.apache.parquet.hadoop.ParquetInputSplit;
//#endif
//#if shim_type=="CDH" || shim_type=="MAPR"
//$import parquet.hadoop.ParquetInputSplit;
//#endif
import org.junit.Test;
import org.junit.Assert;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBigNumber;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.row.value.ValueMetaTimestamp;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat.IPentahoInputSplit;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat.IPentahoRecordReader;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;
import org.pentaho.hadoop.shim.common.format.parquet.PentahoInputSplitImpl;

public class PentahoParquetInputFormatTest {

  @Test
  public void createRecordReader() throws Exception {

    String parquetFilePath = getClass().getClassLoader().getResource( "sample.pqt" ).toExternalForm();

    PentahoParquetInputFormat pentahoParquetInputFormat = new PentahoParquetInputFormat();
    pentahoParquetInputFormat.setInputFile( getClass().getClassLoader().getResource( "sample.pqt" ).toExternalForm() );
    SchemaDescription schema = pentahoParquetInputFormat.readSchema( parquetFilePath );

    pentahoParquetInputFormat.setSchema( schema );

    ParquetInputSplit parquetInputSplit = Mockito.spy( ParquetInputSplit.class );
    Whitebox.setInternalState( parquetInputSplit, "rowGroupOffsets", new long[] { 4 } );
    Whitebox.setInternalState( parquetInputSplit, "file", new Path( parquetFilePath ) );

    PentahoInputSplitImpl pentahoInputSplit = new PentahoInputSplitImpl( parquetInputSplit );

    IPentahoInputFormat.IPentahoRecordReader recordReader =
        pentahoParquetInputFormat.createRecordReader( pentahoInputSplit );

    Assert.assertNotNull( "recordReader should NOT be null!", recordReader );
    Assert.assertTrue( "recordReader should be instance of IPentahoInputFormat.IPentahoRecordReader",
      recordReader instanceof IPentahoInputFormat.IPentahoRecordReader );
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
      PentahoParquetInputFormat in = new PentahoParquetInputFormat();
      in.setInputFile( "/test test/out.txt" );
    } catch (  Exception e ) {
      exception = e;
    }
    //BACKLOG-19435: NoSuchFileException or IOException (mapr) is expected after this change not URISyntaxException
    Assert.assertTrue( exception instanceof NoSuchFileException || exception instanceof IOException );
  }

  private void readData( String file ) throws Exception {
    RowMeta expectedRowMeta = new RowMeta();
    expectedRowMeta.addValueMeta( new ValueMetaNumber( "fnum" ) );
    expectedRowMeta.addValueMeta( new ValueMetaString( "fstring" ) );
    expectedRowMeta.addValueMeta( new ValueMetaDate( "fdate" ) );
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

    PentahoParquetInputFormat inSchema = new PentahoParquetInputFormat();
    SchemaDescription fileSchema =
        inSchema.readSchema( getClass().getClassLoader().getResource( file ).toExternalForm() );

    List<SchemaDescription.Field> fileFields = new ArrayList<>();
    fileSchema.forEach( fileFields::add );

    // fix after autodetection
    Assert.assertEquals( ValueMetaInterface.TYPE_NUMBER, fileFields.get( 5 ).pentahoValueMetaType );
    fileFields.get( 5 ).pentahoValueMetaType = ValueMetaInterface.TYPE_BIGNUMBER;
    Assert.assertEquals( ValueMetaInterface.TYPE_DATE, fileFields.get( 6 ).pentahoValueMetaType );
    fileFields.get( 6 ).pentahoValueMetaType = ValueMetaInterface.TYPE_TIMESTAMP;

    // check fields from file
    Assert.assertEquals( expectedRowMeta.size(), fileFields.size() );
    for ( int i = 0; i < expectedRowMeta.size(); i++ ) {
      ValueMetaInterface vmExpected = expectedRowMeta.getValueMeta( i );
      Assert.assertEquals( vmExpected.getName(), fileFields.get( i ).formatFieldName );
      Assert.assertEquals( vmExpected.getType(), fileFields.get( i ).pentahoValueMetaType );
      Assert.assertEquals( true, fileFields.get( i ).allowNull );
    }

    // check read by one field
    for ( int i = 0; i < fileFields.size(); i++ ) {
      List<ValueMetaInterface> expectedFields = new ArrayList<>();
      List<Object[]> expectedRows = new ArrayList<>();
      SchemaDescription readSchema = new SchemaDescription();

      expectedFields.add( expectedRowMeta.getValueMeta( i ) );
      expectedRows.add( expectedData.get( i ) );
      readSchema.addField( fileFields.get( i ) );

      List<RowMetaAndData> rows = readFile( file, readSchema );
      checkRows( expectedFields, expectedRows, rows );
    }
    // check read in random fields order - 3 times
    Random random = new Random();
    for ( int r = 0; r < 5; r++ ) {
      List<ValueMetaInterface> expectedFields = new ArrayList<>();
      List<Object[]> expectedRows = new ArrayList<>();
      SchemaDescription readSchema = new SchemaDescription();

      int count = expectedRowMeta.getValueMetaList().size();
      Set<Integer> used = new TreeSet<>();
      for ( int i = 0; i < count; i++ ) {
        int rv = random.nextInt( count );
        if ( used.add( rv ) ) {
          expectedFields.add( expectedRowMeta.getValueMeta( rv ) );
          expectedRows.add( expectedData.get( rv ) );
          readSchema.addField( fileFields.get( rv ) );
        }
      }

      List<RowMetaAndData> rows = readFile( file, readSchema );
      checkRows( expectedFields, expectedRows, rows );
    }
  }

  private List<RowMetaAndData> readFile( String file, SchemaDescription readSchema ) throws Exception {
    System.out.println( "Read '" + file + "' as schema: " + readSchema );
    PentahoParquetInputFormat in = new PentahoParquetInputFormat();
    in.setInputFile( getClass().getClassLoader().getResource( file ).toExternalForm() );
    in.setSchema( readSchema );

    List<RowMetaAndData> rows = new ArrayList<>();
    for ( IPentahoInputSplit split : in.getSplits() ) {
      IPentahoRecordReader rd = in.createRecordReader( split );
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
        Assert.assertEquals( expectedRows.get( i )[r], row.getData()[i] );
      }
    }
  }
}
