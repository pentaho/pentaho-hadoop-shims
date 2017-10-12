/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.common.format.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;
import org.pentaho.hadoop.shim.api.format.SchemaDescription.Field;

@RunWith( MockitoJUnitRunner.class )
public class PentahoAvroRecordWriterTest {

  private String schemaString =
      "{"
      + " \"namespace\" : \"TestNameSpace12\","
      + " \"type\" : \"record\","
      + " \"name\" : \"TestnameRecord12\","
      + " \"doc\" : \"TestDocvalue12\","
      + " \"fields\" : [ {"
      + "                   \"name\" : \"stringField\","
      + "                   \"type\" : [ \"null\", \"string\" ]"
      + "                }, {"
      + "                   \"name\" : \"inetField\","
      + "                   \"type\" : \"string\","
      + "                   \"default\" : \"www.uber.com\""
      + "                }, {"
      + "                   \"name\" : \"intField\","
      + "                   \"type\" : [ \"null\", \"long\" ]"
      + "                }, {"
      + "                   \"name\" : \"numberField\","
      + "                   \"type\" : [ \"null\", \"double\" ]"
      + "                }, {"
      + "                   \"name\" : \"bigNumberField\","
      + "                   \"type\" : [ \"null\", \"double\" ]"
      + "                }, {"
      + "                   \"name\" : \"timestampField\","
      + "                   \"type\" : \"long\","
      + "                   \"logicalType\" : \"timestamp-millis\","
      + "                   \"default\" : \"01/01/1970 04:00:00\""
      + "                }, {"
      + "                   \"name\" : \"dateField\","
      + "                   \"type\" : \"int\","
      + "                   \"logicalType\" : \"date\","
      + "                   \"default\" : \"01/02/1970\""
      + "                }, {"
      + "                   \"name\" : \"booleanField\","
      + "                   \"type\" : \"boolean\","
      + "                   \"default\" : \"false\""
      + "                }, {"
      + "                   \"name\" : \"binField\","
      + "                   \"type\" : \"bytes\""
      + "               } ]"
      + "}";

  @Mock private DataFileWriter<GenericRecord> nativeAvroRecordWriter;

  @Mock private ValueMetaInterface vmi;
  @Mock private RowMetaInterface rmi;
  @Spy private RowMetaAndData rmd;

  private SchemaDescription schemaDescription;
  private PentahoAvroRecordWriter writer;

  @Before
  public void setUp() throws IOException, URISyntaxException {
    InputStream is = new ByteArrayInputStream( schemaString.getBytes( "UTF-8" ) );
    Schema schema = new Schema.Parser().parse( is );

    schemaDescription = new SchemaDescription();

    when( rmi.getValueMeta( anyInt() ) ).thenReturn( vmi );
    rmd.setRowMeta( rmi );

    writer = new PentahoAvroRecordWriter( nativeAvroRecordWriter, schema, schemaDescription );
  }

  @Test
  public void testWrite_String() throws KettleValueException, IOException {
    doReturn( "sampleString" ).when( rmd ).getString( anyInt(), anyString() );
    testWriteCommon( ValueMetaInterface.TYPE_STRING, "stringField", "sampleString" );
  }

  @Test
  public void testWrite_Inet() throws KettleValueException, IOException {
    doReturn( "sampleInet" ).when( rmd ).getString( anyInt(), anyString() );
    testWriteCommon( ValueMetaInterface.TYPE_INET, "inetField", "sampleInet" );
  }

  @Test
  public void testWrite_Integer() throws KettleValueException, IOException {
    doReturn( 0L ).when( rmd ).getInteger( anyInt(), anyLong() );
    doReturn( 0L ).when( rmd ).getInteger( anyInt() );
    testWriteCommon( ValueMetaInterface.TYPE_INTEGER, "intField", 0L );
  }

  @Test
  public void testWrite_Number() throws KettleValueException, IOException {
    doReturn( 0d ).when( rmd ).getNumber( anyInt(), anyLong() );
    testWriteCommon( ValueMetaInterface.TYPE_NUMBER, "numberField", 0d );
  }

  @Test
  public void testWrite_BigNumber() throws KettleValueException, IOException {
    doReturn( new BigDecimal( 0d ) ).when( rmd ).getBigNumber( anyInt(), any( BigDecimal.class ) );
    testWriteCommon( ValueMetaInterface.TYPE_BIGNUMBER, "bigNumberField", new BigDecimal( 0d ).doubleValue() );
  }

  @Test
  public void testWrite_Timestamp() throws KettleValueException, IOException {
    doReturn( new Date( 0 ) ).when( rmd ).getDate( anyInt(), any( Date.class ) );
    testWriteCommon( ValueMetaInterface.TYPE_TIMESTAMP, "timestampField", 0L );
  }

  @Test
  public void testWrite_Date() throws KettleValueException, IOException {
    Date dateFromRow = new Date( 0 );
    LocalDate rowDate = dateFromRow.toInstant().atZone( ZoneId.systemDefault() ).toLocalDate();
    doReturn( dateFromRow ).when( rmd ).getDate( anyInt(), any( Date.class ) );
    testWriteCommon( ValueMetaInterface.TYPE_DATE, "dateField", Math.toIntExact( ChronoUnit.DAYS.between( LocalDate.ofEpochDay( 0 ), rowDate ) ) );
  }

  @Test
  public void testWrite_Boolean() throws KettleValueException, IOException {
    doReturn( true ).when( rmd ).getBoolean( anyInt(), anyBoolean() );
    testWriteCommon( ValueMetaInterface.TYPE_BOOLEAN, "booleanField", true );
  }

  @Test
  public void testWrite_Binary() throws KettleValueException, IOException {
    doReturn( new byte[0] ).when( rmd ).getBinary( anyInt(), any( byte[].class ) );
    when( vmi.getType() ).thenReturn( ValueMetaInterface.TYPE_BINARY );

    Field field = schemaDescription.new Field( "binField", "pentaho_binField", ValueMetaInterface.TYPE_BINARY, false );
    schemaDescription.addField( field );
    writer.write( rmd );

    ArgumentCaptor<GenericRecord> argument = ArgumentCaptor.forClass( GenericRecord.class );
    verify( nativeAvroRecordWriter ).append( argument.capture() );
    assertEquals( ByteBuffer.wrap( new byte[0] ), argument.getValue().get( "binField" ) );
  }

  private void testWriteCommon( int type, String fieldName, Object writableObject ) throws KettleValueException, IOException {
    when( vmi.getType() ).thenReturn( type );

    Field field = schemaDescription.new Field( fieldName, "pentaho_" + fieldName, type, false );
    schemaDescription.addField( field );
    writer.write( rmd );

    ArgumentCaptor<GenericRecord> argument = ArgumentCaptor.forClass( GenericRecord.class );
    verify( nativeAvroRecordWriter ).append( argument.capture() );
    assertEquals( writableObject, argument.getValue().get( fieldName ) );
  }

  @Test
  public void testWrite_String_Default() throws KettleValueException, IOException {
    when( rmi.getString( any( Object[].class ), anyInt() ) ).thenReturn( null );
    testWriteCommon_Default( ValueMetaInterface.TYPE_STRING, "stringField", "sampleString", "defaultString", "defaultString" );
  }

  @Test
  public void testWrite_Inet_Default() throws KettleValueException, IOException {
    when( rmi.getString( any( Object[].class ), anyInt() ) ).thenReturn( null );
    testWriteCommon_Default( ValueMetaInterface.TYPE_INET, "inetField", "sampleInet", "defaultInet", "defaultInet" );
  }

  @Test
  public void testWrite_Integer_Default() throws KettleValueException, IOException {
    when( rmi.getInteger( any( Object[].class ), anyInt() ) ).thenReturn( null );
    testWriteCommon_Default( ValueMetaInterface.TYPE_INTEGER, "intField", 0L, String.valueOf( 1L ), 1L );
  }

  @Test
  public void testWrite_Number_Default() throws KettleValueException, IOException {
    when( rmi.getNumber( any( Object[].class ), anyInt() ) ).thenReturn( null );
    testWriteCommon_Default( ValueMetaInterface.TYPE_NUMBER, "numberField", 0d, String.valueOf( 1d ), 1d );
  }

  @Test
  public void testWrite_BigNumber_Default() throws KettleValueException, IOException {
    when( rmi.getBigNumber( any( Object[].class ), anyInt() ) ).thenReturn( null );
    testWriteCommon_Default( ValueMetaInterface.TYPE_BIGNUMBER, "bigNumberField", 0d, String.valueOf( 1d ), 1d );
  }

  @Test
  public void testWrite_Timestamp_Default() throws KettleValueException, IOException {
    when( rmi.getDate( any( Object[].class ), anyInt() ) ).thenReturn( null );
    //will set time zone since we use strong equals to 1L
    when( vmi.getConversionMask() ).thenReturn( "MM/dd/yyyy HH:mm:ss.SSS Z" );
    testWriteCommon_Default( ValueMetaInterface.TYPE_TIMESTAMP, "timestampField", 0l, "01/01/1970 00:00:00.001 -0000", 1l );
  }

  @Test
  public void testWrite_Date_Default() throws KettleValueException, IOException {
    when( rmi.getDate( any( Object[].class ), anyInt() ) ).thenReturn( null );
    when( vmi.getConversionMask() ).thenReturn( "MM/dd/yyyy" );
    testWriteCommon_Default( ValueMetaInterface.TYPE_DATE, "dateField", 0, "01/02/1970", 1 );
  }

  @Test
  public void testWrite_Boolean_Default() throws KettleValueException, IOException {
    when( rmi.getBoolean( any( Object[].class ), anyInt() ) ).thenReturn( null );
    testWriteCommon_Default( ValueMetaInterface.TYPE_BOOLEAN, "booleanField", true, String.valueOf( false ), false );
  }

  @Test
  public void shouldCloseNativeWriter() throws Exception {
    writer.close();
    verify( nativeAvroRecordWriter ).close();
  }

  private void testWriteCommon_Default( int type, String fieldName, Object writableObject, String defaultValue, Object defaultObject ) throws KettleValueException, IOException {
    when( vmi.getType() ).thenReturn( type );

    Field field = schemaDescription.new Field( fieldName, "pentaho_" + fieldName, type, false );
    field.defaultValue = defaultValue;
    schemaDescription.addField( field );
    writer.write( rmd );

    ArgumentCaptor<GenericRecord> argument = ArgumentCaptor.forClass( GenericRecord.class );
    verify( nativeAvroRecordWriter ).append( argument.capture() );
    assertNotEquals( writableObject, argument.getValue().get( fieldName ) );
    assertEquals( defaultObject, argument.getValue().get( fieldName ) );
  }

}
