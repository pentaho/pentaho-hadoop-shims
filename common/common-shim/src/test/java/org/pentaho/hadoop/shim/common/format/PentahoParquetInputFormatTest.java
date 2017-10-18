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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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

    PentahoParquetInputFormat in = new PentahoParquetInputFormat();
    System.out.println( "Read file from " + getClass().getClassLoader().getResource( file ) );
    SchemaDescription fileSchema = in.readSchema( getClass().getClassLoader().getResource( file ).toExternalForm() );

    List<SchemaDescription.Field> fileFields = new ArrayList<>();
    fileSchema.forEach( fileFields::add );
    Assert.assertEquals( 7, fileFields.size() );
    Assert.assertEquals( "fnum", fileFields.get( 0 ).formatFieldName );
    Assert.assertEquals( ValueMetaInterface.TYPE_NUMBER, fileFields.get( 0 ).pentahoValueMetaType );
    Assert.assertEquals( true, fileFields.get( 0 ).allowNull );
    Assert.assertEquals( "fstring", fileFields.get( 1 ).formatFieldName );
    Assert.assertEquals( ValueMetaInterface.TYPE_STRING, fileFields.get( 1 ).pentahoValueMetaType );
    Assert.assertEquals( true, fileFields.get( 1 ).allowNull );
    Assert.assertEquals( "fdate", fileFields.get( 2 ).formatFieldName );
    Assert.assertEquals( ValueMetaInterface.TYPE_DATE, fileFields.get( 2 ).pentahoValueMetaType );
    Assert.assertEquals( true, fileFields.get( 2 ).allowNull );
    Assert.assertEquals( "fbool", fileFields.get( 3 ).formatFieldName );
    Assert.assertEquals( ValueMetaInterface.TYPE_BOOLEAN, fileFields.get( 3 ).pentahoValueMetaType );
    Assert.assertEquals( true, fileFields.get( 3 ).allowNull );
    Assert.assertEquals( "fint", fileFields.get( 4 ).formatFieldName );
    Assert.assertEquals( ValueMetaInterface.TYPE_INTEGER, fileFields.get( 4 ).pentahoValueMetaType );
    Assert.assertEquals( true, fileFields.get( 4 ).allowNull );
    Assert.assertEquals( "fbignum", fileFields.get( 5 ).formatFieldName );
    Assert.assertEquals( ValueMetaInterface.TYPE_NUMBER, fileFields.get( 5 ).pentahoValueMetaType );
    Assert.assertEquals( true, fileFields.get( 5 ).allowNull );
    Assert.assertEquals( "ftime", fileFields.get( 6 ).formatFieldName );
    Assert.assertEquals( ValueMetaInterface.TYPE_DATE, fileFields.get( 6 ).pentahoValueMetaType );
    Assert.assertEquals( true, fileFields.get( 6 ).allowNull );

    fileFields.get( 6 ).pentahoValueMetaType = ValueMetaInterface.TYPE_BIGNUMBER;
    fileFields.get( 6 ).pentahoValueMetaType = ValueMetaInterface.TYPE_TIMESTAMP;

    in.setInputFile( getClass().getClassLoader().getResource( file ).toExternalForm() );
    in.setSchema( fileSchema );

    List<RowMetaAndData> rows = new ArrayList<>();
    for ( IPentahoInputSplit split : in.getSplits() ) {
      IPentahoRecordReader rd = in.createRecordReader( split );
      rd.forEach( row -> rows.add( row ) );
      rd.close();
    }

    Assert.assertEquals( 4, rows.size() );
    for ( RowMetaAndData row : rows ) {
      ValueMetaInterface fnum = row.getRowMeta().getValueMeta( 0 );
      ValueMetaInterface fstring = row.getRowMeta().getValueMeta( 1 );
      ValueMetaInterface fdate = row.getRowMeta().getValueMeta( 2 );
      ValueMetaInterface fbool = row.getRowMeta().getValueMeta( 3 );
      ValueMetaInterface fint = row.getRowMeta().getValueMeta( 4 );
      ValueMetaInterface fbignum = row.getRowMeta().getValueMeta( 5 );
      ValueMetaInterface ftime = row.getRowMeta().getValueMeta( 6 );

      Assert.assertEquals( "fnum", fnum.getName() );
      Assert.assertEquals( ValueMetaInterface.TYPE_NUMBER, fnum.getType() );
      Assert.assertEquals( "fstring", fstring.getName() );
      Assert.assertEquals( ValueMetaInterface.TYPE_STRING, fstring.getType() );
      Assert.assertEquals( "fdate", fdate.getName() );
      Assert.assertEquals( ValueMetaInterface.TYPE_DATE, fdate.getType() );
      Assert.assertEquals( "fbool", fbool.getName() );
      Assert.assertEquals( ValueMetaInterface.TYPE_BOOLEAN, fbool.getType() );
      Assert.assertEquals( "fint", fint.getName() );
      Assert.assertEquals( ValueMetaInterface.TYPE_INTEGER, fint.getType() );
      Assert.assertEquals( "fbignum", fbignum.getName() );
      // Assert.assertEquals( ValueMetaInterface.TYPE_BIGNUMBER, fbignum.getType() ); -- double inside
      Assert.assertEquals( "ftime", ftime.getName() );
      Assert.assertEquals( ValueMetaInterface.TYPE_TIMESTAMP, ftime.getType() );
    }

    testRow( rows.get( 0 ).getData(), 2.1, "John", new Date( 456 ), true, 1L, 4.5, null );
    testRow( rows.get( 1 ).getData(), null, "Paul", new Date( 456 ), false, 3L, null, new Timestamp( 123 ) );
    testRow( rows.get( 2 ).getData(), 2.1, "George", null, true, null, 4.5, new Timestamp( 123 ) );
    testRow( rows.get( 3 ).getData(), 2.1, "Ringo", new Date( 456 ), null, 4L, 4.5, new Timestamp( 123 ) );
  }

  private void testRow( Object[] data, Object... expected ) {
    for ( int i = 0; i < expected.length; i++ ) {
      Assert.assertEquals( expected[i], data[i] );
    }
  }
}
