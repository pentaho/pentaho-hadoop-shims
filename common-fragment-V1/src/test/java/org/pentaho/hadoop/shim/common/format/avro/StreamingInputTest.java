/*! ******************************************************************************
 *
 * Pentaho Big Data
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
package org.pentaho.hadoop.shim.common.format.avro;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBigNumber;
import org.pentaho.di.core.row.value.ValueMetaBinary;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaInternetAddress;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaPluginType;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.row.value.ValueMetaTimestamp;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.format.AvroSpec;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class StreamingInputTest {
  private DateFormat dateFormat = new SimpleDateFormat( "yyyy/MM/dd HH:mm:ss.SSS" );

  @Test
  public void testStreamingInput() throws Exception {
    String filePath = "/avro/StreamingInputTest.avro";
    List<AvroInputField> avroInputFields = Arrays.asList( new AvroInputField[] {
      buildAvroInputField( "FirstName", AvroSpec.DataType.STRING, ValueMetaInterface.TYPE_STRING ),
      buildAvroInputField( "LastName", AvroSpec.DataType.STRING, ValueMetaInterface.TYPE_STRING ),
      buildAvroInputField( "theDate", AvroSpec.DataType.DATE, ValueMetaInterface.TYPE_DATE ),
      buildAvroInputField( "theInt", AvroSpec.DataType.INTEGER, ValueMetaInterface.TYPE_INTEGER ),
      buildAvroInputField( "theNumber", AvroSpec.DataType.DOUBLE, ValueMetaInterface.TYPE_NUMBER ),
      buildAvroInputField( "theBinary", AvroSpec.DataType.BYTES, ValueMetaInterface.TYPE_BINARY ),
      buildAvroInputField( "theBoolean", AvroSpec.DataType.BOOLEAN, ValueMetaInterface.TYPE_BOOLEAN ),
      buildAvroInputField( "theTimestamp", AvroSpec.DataType.TIMESTAMP_MILLIS, ValueMetaInterface.TYPE_TIMESTAMP ),
      buildAvroInputField( "theInet", AvroSpec.DataType.STRING, ValueMetaInterface.TYPE_INET ),
      buildAvroInputField( "theDecimal", AvroSpec.DataType.DECIMAL, ValueMetaInterface.TYPE_BIGNUMBER, 20, 10 )
    } );

    Date date1 = ( dateFormat.parse( "1999/01/01 00:00:00.000" ) );
    dateFormat.setTimeZone( TimeZone.getTimeZone( "EST" ) ); //The test file was written with EST time zone
    Timestamp timeStamp1 = new Timestamp( dateFormat.parse( "2001/12/01 00:00:00.000" ).getTime() );

    Object[] expectedResults = new Object[] {
      "John", "Smith", date1, 1L, 1.1D, "foobar".getBytes(), false, timeStamp1,
      InetAddress.getByName( "www.pentaho.com" ), new BigDecimal( "1234567890.0987654321" )
    };

    testStreamingRecordReader( avroInputFields, null, filePath, expectedResults );
  }

  @Test
  public void testStreamingInputWithIncomingRow() throws Exception {
    String filePath = "/avro/StreamingInputTest.avro";
    List<AvroInputField> avroInputFields = Arrays.asList( new AvroInputField[] {
      buildAvroInputField( "FirstName", AvroSpec.DataType.STRING, ValueMetaInterface.TYPE_STRING ),
      buildAvroInputField( "LastName", AvroSpec.DataType.STRING, ValueMetaInterface.TYPE_STRING ),
      buildAvroInputField( "theDate", AvroSpec.DataType.DATE, ValueMetaInterface.TYPE_DATE ),
      buildAvroInputField( "theInt", AvroSpec.DataType.INTEGER, ValueMetaInterface.TYPE_INTEGER ),
      buildAvroInputField( "theNumber", AvroSpec.DataType.DOUBLE, ValueMetaInterface.TYPE_NUMBER ),
      buildAvroInputField( "theBinary", AvroSpec.DataType.BYTES, ValueMetaInterface.TYPE_BINARY ),
      buildAvroInputField( "theBoolean", AvroSpec.DataType.BOOLEAN, ValueMetaInterface.TYPE_BOOLEAN ),
      buildAvroInputField( "theTimestamp", AvroSpec.DataType.TIMESTAMP_MILLIS, ValueMetaInterface.TYPE_TIMESTAMP ),
      buildAvroInputField( "theInet", AvroSpec.DataType.STRING, ValueMetaInterface.TYPE_INET ),
      buildAvroInputField( "theDecimal", AvroSpec.DataType.DECIMAL, ValueMetaInterface.TYPE_BIGNUMBER, 20, 10 )
    } );

    Date date1 = ( dateFormat.parse( "1999/01/01 00:00:00.000" ) );
    dateFormat.setTimeZone( TimeZone.getTimeZone( "EST" ) ); //The test file was written with EST time zone
    Timestamp timeStamp1 = new Timestamp( dateFormat.parse( "2001/12/01 00:00:00.000" ).getTime() );

    Object[] expectedResults = new Object[] {
      "John", "Smith", date1, 1L, 1.1D, "foobar".getBytes(), false, timeStamp1,
      InetAddress.getByName( "www.pentaho.com" ), new BigDecimal( "1234567890.0987654321" )
    };

    Object[] incomingRowData = new Object[] { "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10" };
    Object[] compareIncomingRowData = new Object[] { "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10" };

    testStreamingRecordReader( avroInputFields, incomingRowData, filePath, expectedResults );
    for ( int i = 0; i < compareIncomingRowData.length; i++ ) {
      assertEquals( compareIncomingRowData[ i ], incomingRowData[ i ] );
    }
  }

  private AvroInputField buildAvroInputField( String name, AvroSpec.DataType avroType, int pentahoType ) {
    return buildAvroInputField( name, avroType, pentahoType, 0, 0 );
  }

  private AvroInputField buildAvroInputField( String name, AvroSpec.DataType avroType, int pentahoType, int precision,
                                              int scale ) {
    AvroInputField avroInputField = new AvroInputField();
    avroInputField.setAvroFieldName( name );
    avroInputField.setPentahoFieldName( name );
    avroInputField.setAvroType( avroType );
    avroInputField.setPentahoType( pentahoType );
    avroInputField.setPrecision( precision );
    avroInputField.setScale( scale );
    return avroInputField;
  }

  private void testStreamingRecordReader( List<AvroInputField> avroInputFields,
                                          Object[] origValues, String filePath, Object[] expectedResults )
    throws Exception {

    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.init( true );

    ByteArrayInputStream inputStream = fileToByteArrayInputStream( filePath );

    PentahoAvroInputFormat pentahoAvroInputFormat = new PentahoAvroInputFormat( mock( NamedCluster.class ) );

    pentahoAvroInputFormat.setInputFields( avroInputFields );
    pentahoAvroInputFormat.setInputStreamFieldName( "stream" );
    pentahoAvroInputFormat.setIsDataBinaryEncoded( true );
    pentahoAvroInputFormat.setUseFieldAsInputStream( true );
    if ( origValues != null ) {
      pentahoAvroInputFormat.setIncomingFields( origValues );
    }
    RowMeta inRowMeta = new RowMeta();
    inRowMeta.addValueMeta( new ValueMetaString( "stream" ) );
    pentahoAvroInputFormat.setIncomingRowMeta( inRowMeta );

    RowMeta outRowMeta = new RowMeta();
    for ( AvroInputField avroInputField : avroInputFields ) {
      outRowMeta.addValueMeta(
        getValueMetaInterface( avroInputField.getPentahoFieldName(), avroInputField.getPentahoType() )
      );
    }

    pentahoAvroInputFormat.setOutputRowMeta( outRowMeta );
    pentahoAvroInputFormat.setInputStream( inputStream );

    IPentahoInputFormat.IPentahoRecordReader pentahoRecordReader = pentahoAvroInputFormat.createRecordReader( null );
    for ( RowMetaAndData row : pentahoRecordReader ) {
      for ( int colNum = 0; colNum < avroInputFields.size(); colNum++ ) {
        Object expectedValue = ( expectedResults == null ? origValues[ colNum ] : expectedResults[ colNum ] );
        Object actualValue = row.getData()[ colNum ];
        AvroInputField avroInputField = avroInputFields.get( colNum );

        String errMsg = "field " + colNum + " does not match in " + row;

        // If here we are comparing read value with the expected value
        if ( expectedValue instanceof BigDecimal ) {
          assert ( ( (BigDecimal) expectedValue ).compareTo(
            (BigDecimal) actualValue ) == 0 );
        } else if ( expectedValue instanceof byte[] ) {
          assertEquals( errMsg, new String( (byte[]) expectedValue ),
            new String( (byte[]) actualValue ) );
        } else if ( expectedValue instanceof InetAddress ) {
          //The inet address is too dynamic to check
          System.out.println( "Skipping INET check" );
          //byte[] origAddress = ( (InetAddress) expectedValue ).getAddress();
          //byte[] readAddress = ( (InetAddress) actualValue ).getAddress();
          //assertEquals( errMsg, new String( origAddress ), new String( readAddress ) );
        } else {
          assertEquals( errMsg, expectedValue, actualValue );
        }

      }
    }
  }

  private ByteArrayInputStream fileToByteArrayInputStream( String filePath ) throws Exception {
    File file = new File( getClass().getResource( filePath ).getFile() );
    return new ByteArrayInputStream( FileUtils.readFileToByteArray( file ) );
  }

  private ValueMetaInterface getValueMetaInterface( String fieldName, int fieldType ) {
    switch ( fieldType ) {
      case ValueMetaInterface.TYPE_INET:
        return new ValueMetaInternetAddress( fieldName );
      case ValueMetaInterface.TYPE_STRING:
        return new ValueMetaString( fieldName );
      case ValueMetaInterface.TYPE_INTEGER:
        return new ValueMetaInteger( fieldName );
      case ValueMetaInterface.TYPE_NUMBER:
        return new ValueMetaNumber( fieldName );
      case ValueMetaInterface.TYPE_BIGNUMBER:
        return new ValueMetaBigNumber( fieldName );
      case ValueMetaInterface.TYPE_TIMESTAMP:
        ValueMetaTimestamp valueMetaTimestamp = new ValueMetaTimestamp( fieldName );
        valueMetaTimestamp.setConversionMask( "yyyy/MM/dd HH:mm:ss.SSS" );
        return valueMetaTimestamp;
      case ValueMetaInterface.TYPE_DATE:
        ValueMetaDate valueMetaDate = new ValueMetaDate( fieldName );
        valueMetaDate.setConversionMask( "yyyy/MM/dd HH:mm:ss.SSS" );
        return valueMetaDate;
      case ValueMetaInterface.TYPE_BOOLEAN:
        return new ValueMetaBoolean( fieldName );
      case ValueMetaInterface.TYPE_BINARY:
        return new ValueMetaBinary( fieldName );
    }
    return null;
  }
}
