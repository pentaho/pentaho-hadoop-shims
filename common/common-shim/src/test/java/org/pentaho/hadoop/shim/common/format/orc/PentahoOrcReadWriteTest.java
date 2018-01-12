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
package org.pentaho.hadoop.shim.common.format.orc;

import org.apache.orc.TypeDescription;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
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
import org.pentaho.di.core.util.Assert;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcOutputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;
import org.pentaho.hadoop.shim.common.format.PentahoOrcOutputFormat;

import java.math.BigDecimal;
import java.math.MathContext;
import java.net.InetAddress;
import java.nio.file.FileAlreadyExistsException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


/**
 * Created by tkafalas on 11/3/2017.
 */
public class PentahoOrcReadWriteTest {
  private SchemaDescription schemaDescription;
  private RowMeta rowMeta;
  private Object[][] rowData;
  private PentahoOrcOutputFormat orcOutputFormat;
  private String filePath;
  private DateFormat dateFormat = new SimpleDateFormat( "yyyy/MM/dd HH:mm:ss.SSS" );

  private InetAddress inetAddress1;

  {
    try {
      inetAddress1 = InetAddress.getByName( "www.pentaho.com" );
    } catch ( Exception e ){
      //should not happen
    }
  }

  //This is the metadata and default values we will use to test with.  We'll create a file with two sets of these
  // 10 fields.  The first set will allows nulls, the seconds set will not allow nulls and use the default values.
  // The second set of fields will have "def" appended to all field names to keep them unique.
  private final String[][] schemaData = new String[][] {
    { "orcField1", "pentahoField1", String.valueOf( ValueMetaInterface.TYPE_STRING ), "default" },
    { "orcField2", "pentahoField2", String.valueOf( ValueMetaInterface.TYPE_STRING ), "default2" },
    { "orcDouble3", "pentahoNumber3", String.valueOf( ValueMetaInterface.TYPE_NUMBER ), "1234.0" },
    { "orcDouble4", "pentahoBigNumber4", String.valueOf( ValueMetaInterface.TYPE_BIGNUMBER ), "123456789.98765" },
    { "orcBytes5", "pentahoInet5", String.valueOf( ValueMetaInterface.TYPE_INET ), inetAddress1.getHostAddress() },
    { "orcLong6", "pentahoBoolean6", String.valueOf( ValueMetaInterface.TYPE_BOOLEAN ), "true" },
    { "orcInt7", "pentahoInt7", String.valueOf( ValueMetaInterface.TYPE_INTEGER ), "-33456" },
    { "orcDate8", "pentahoDate8", String.valueOf( ValueMetaInterface.TYPE_DATE ), "1980/01/01 00:00:00.000" },
    { "orcTimestamp9", "pentahoTimestamp9", String.valueOf( ValueMetaInterface.TYPE_TIMESTAMP ),
      "1980/01/01 00:00:00.000" },
    { "orcBytes10", "pentahoBinary10", String.valueOf( ValueMetaInterface.TYPE_BINARY ), "binary" }
  };
  private final int ORC_NAME_INDEX = 0;
  private final int PENTAHO_NAME_INDEX = 1;
  private final int TYPE_INDEX = 2;
  private final int DEFAULT_VALUE_INDEX = 3;


  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setup() throws Exception {
    tempFolder.create();
    orcOutputFormat = new PentahoOrcOutputFormat();

    // Set up the Orc Schema Description and rowMeta for the first set of fields.
    schemaDescription = new SchemaDescription();
    rowMeta = new RowMeta();
    for ( String[] schemaField : schemaData ) {
      schemaDescription
        .addField( schemaDescription.new Field( schemaField[ ORC_NAME_INDEX ], schemaField[ PENTAHO_NAME_INDEX ],
          Integer.valueOf( schemaField[ TYPE_INDEX ] ), true ) );
      addFieldToRowMeta( schemaField[ PENTAHO_NAME_INDEX ], Integer.valueOf( schemaField[ TYPE_INDEX ] ) );
    }
    // Set up the Orc Schema Description and rowMeta for the second set of fields.
    for ( String[] schemaField : schemaData ) {
      schemaDescription.addField( schemaDescription.new Field( schemaField[ ORC_NAME_INDEX ] + "Def",
        schemaField[ PENTAHO_NAME_INDEX ] + "Def", Integer.valueOf( schemaField[ TYPE_INDEX ] ),
        schemaField[ DEFAULT_VALUE_INDEX ], false ) );
      addFieldToRowMeta( schemaField[ PENTAHO_NAME_INDEX ] + "Def", Integer.valueOf( schemaField[ TYPE_INDEX ] ) );
    }

    // Then set that schemaDescription on the orc output format.
    orcOutputFormat.setSchemaDescription( schemaDescription );
    Date date1 = ( dateFormat.parse( "2001/11/01 00:00:00.000" ) );
    Date date2 = ( dateFormat.parse( "1999/12/31 00:00:00.000" ) );
    Date timeStamp1 = new Timestamp( dateFormat.parse( "2001/11/01 20:30:15.123" ).getTime() );
    Date timeStamp2 = new Timestamp( dateFormat.parse( "1999/12/31 23:59:59.999" ).getTime() );

    // Populate three rows of values.  These are values we will write rows with.  Whe we read back the rows from the
    // file they will be compared with these values to know it made the round trip.
    rowData = new Object[][]
      { { "Row1Field1", "Row1Field2", 3.1, new BigDecimal( 4.1, MathContext.DECIMAL64 ),
        InetAddress.getByName( "www.microsoft.com" ), true, 1L, date1, timeStamp1, "foobar".getBytes(),
        "Row1Field3", "Row1Field4", 3.1, new BigDecimal( 4.1, MathContext.DECIMAL64 ),
        InetAddress.getByName( "www.microsoft.com" ), true, 1L, date1, timeStamp2, "foobar".getBytes() },

        { "Row2Field1", "Row2Field2", -3.2, new BigDecimal( -4.2, MathContext.DECIMAL64 ),
          InetAddress.getByName( "www.microsoft.com" ), false, -2L, date2, timeStamp2, "Donald Duck".getBytes(),
          "Row2Field3", "Row2Field4", -3.2, new BigDecimal( -4.2, MathContext.DECIMAL64 ),
          InetAddress.getByName( "www.microsoft.com" ), false, -2L, date2, timeStamp2, "Donald Duck".getBytes()
        },

        { "Row3Field1", null, null, null, null, null, null, null, null, null,
          "Row3Field4", null, null, null, null, null, null, null, null, null }
      };
  }

  @Test
  public void testOrcFileWriteAndRead() throws Exception {
    doReadWrite( IPentahoOrcOutputFormat.COMPRESSION.NONE, "orcOutputNone.orc", false );
    doReadWrite( IPentahoOrcOutputFormat.COMPRESSION.SNAPPY, "orcOutputSnappy.orc", false );
    doReadWrite( IPentahoOrcOutputFormat.COMPRESSION.ZLIB, "orcOutputZlib.orc", false );
    //#if shim_type!="EMR"
    //$doReadWrite( IPentahoOrcOutputFormat.COMPRESSION.LZO, "orcOutputLzo.orc", false );
    //#endif

  }

  @Test(expected = FileAlreadyExistsException.class)
  public void testOverwriteFileIsFalse() throws Exception {
    doReadWrite( IPentahoOrcOutputFormat.COMPRESSION.NONE, "orcOutputNone.orc", false );
    doReadWrite( IPentahoOrcOutputFormat.COMPRESSION.NONE, "orcOutputNone.orc", false );
  }

  @Test
  public void testOverwriteFileIsTrue() throws Exception {
    doReadWrite( IPentahoOrcOutputFormat.COMPRESSION.NONE, "orcOutputNone.orc", false );
    doReadWrite( IPentahoOrcOutputFormat.COMPRESSION.NONE, "orcOutputNone.orc", true );
  }

  private void doReadWrite( IPentahoOrcOutputFormat.COMPRESSION compressionType, String outputFileName, boolean overwriteFile )
    throws Exception {
    orcOutputFormat.setCompression( compressionType );
    if (tempFolder.getRoot().toString().substring( 1, 2 ).equals( ":" )) {
      filePath = tempFolder.getRoot().toString().substring( 2 ) + "/" + outputFileName;
    } else {
      filePath = tempFolder.getRoot().toString() + "/" + outputFileName;
    }
    filePath = filePath.replace( "\\", "/" );

    try {
      orcOutputFormat.setOutputFile( filePath, overwriteFile );
    } catch ( FileAlreadyExistsException e) {
      throw e;
    } catch ( Exception e ) {
      e.printStackTrace();
    }
    //Write the Orc File
    System.out.println( "Writing file " + filePath );
    testRecordWriter();
    //Read it back and check values
    System.out.println( "Reading file " + filePath );
    testRecordReader();
    //Test that we can extract the schema (TypeDescription) from the file
    System.out.println( "reading Schema " + filePath );
    testGetSchema();
  }

  /**
   * Write all the rows in the rowData array to an orc file
   *
   * @throws Exception
   */
  private void testRecordWriter() throws Exception {
    IPentahoOutputFormat.IPentahoRecordWriter orcRecordWriter = orcOutputFormat.createRecordWriter();
    Assert.assertNotNull( orcRecordWriter, "orcRecordWriter should NOT be null!" );
    Assert.assertTrue( orcRecordWriter instanceof PentahoOrcRecordWriter,
      "orcRecordWriter should be instance of PentahoOrcRecordWriter" );

    for ( int i = 0; i < rowData.length; i++ ) {
      orcRecordWriter.write( new RowMetaAndData( rowMeta, rowData[ i ] ) );
    }
    try {
      orcRecordWriter.close();
    } catch ( Exception e ) {
      e.printStackTrace();
    }
  }

  /**
   * Read the rows back from Orc file
   *
   * @throws Exception
   */
  private void testRecordReader() throws Exception {

    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.init( true );

    PentahoOrcInputFormat pentahoOrcInputFormat = new PentahoOrcInputFormat();
    pentahoOrcInputFormat.setSchema( schemaDescription );
    pentahoOrcInputFormat.setInputFile( filePath );
    IPentahoInputFormat.IPentahoRecordReader pentahoRecordReader = pentahoOrcInputFormat.createRecordReader( null );
    final AtomicInteger rowNumber = new AtomicInteger();
    for ( RowMetaAndData row : pentahoRecordReader ) {
      final AtomicInteger fieldNumber = new AtomicInteger();
      schemaDescription.forEach( field -> testValue( field, row, rowNumber, fieldNumber ) );
      rowNumber.incrementAndGet();
    }
  }

  private void testGetSchema() throws Exception {
    PentahoOrcInputFormat pentahoOrcInputFormat = new PentahoOrcInputFormat();
    pentahoOrcInputFormat.setInputFile( filePath );

    assertNotNull( "Schema Description should be populated", schemaDescription );
    //If here we hopefully read the the TypeDescription out of the orc file
    final SchemaDescription schemaDesc = pentahoOrcInputFormat.readSchema();
    schemaDescription.forEach( field -> compareField( field, schemaDesc ) );
  }

  private void testValue( SchemaDescription.Field field, RowMetaAndData row, AtomicInteger rowNumber,
                          AtomicInteger fieldNumber ) {
    int fldNum = fieldNumber.getAndIncrement();
    int rowNum = rowNumber.get();
    Object origValue = rowData[ rowNum ][ fldNum ];
    Object readValue = row.getData()[ fldNum ];
    String errMsg = "field " + fldNum + " does not match in " + row;

    if ( origValue == null && field.allowNull == false ) {
      //If here we are comparing the read value to the default value
      if ( field.pentahoValueMetaType == ValueMetaInterface.TYPE_INET ) {
        assertTrue( errMsg, readValue.toString().contains( field.defaultValue ) );
      } else if ( field.pentahoValueMetaType == ValueMetaInterface.TYPE_DATE ) {
        try {
          assertEquals( errMsg, dateFormat.parse( field.defaultValue ).toString(), readValue.toString() );
        } catch ( ParseException e ) {
          e.printStackTrace();
        }
      } else if ( field.pentahoValueMetaType == ValueMetaInterface.TYPE_TIMESTAMP ) {
        try {
          assertEquals( errMsg, new Timestamp( dateFormat.parse( field.defaultValue ).getTime() ).toString(),
            readValue.toString() );
        } catch ( ParseException e ) {
          e.printStackTrace();
        }
      } else if ( field.pentahoValueMetaType == ValueMetaInterface.TYPE_BINARY ) {
        assertEquals( errMsg, field.defaultValue, new String( (byte[]) readValue ) );
      } else {
        assertEquals( errMsg, field.defaultValue, readValue.toString() );
      }
    } else {
      // If here we are comparing read value with the original value
      if ( origValue instanceof BigDecimal ) {
        assert ( ( (BigDecimal) origValue ).compareTo(
          (BigDecimal) readValue ) == 0 );
      } else if ( origValue instanceof byte[] ) {
        assertEquals( errMsg, new String( (byte[]) origValue ),
          new String( (byte[]) readValue ) );
      } else {
        assertEquals( errMsg, origValue, readValue );
      }
    }
  }

  private void compareField( SchemaDescription.Field field, SchemaDescription schemaDesc ) {
    SchemaDescription.Field readField = schemaDesc.getField( field.formatFieldName );
    assertNotNull( "Field " + field.formatFieldName + " should be found in the read schema", readField );
    assertEquals( "Field " + field.formatFieldName, field.pentahoValueMetaType, readField.pentahoValueMetaType );
  }

  private void addFieldToRowMeta( String fieldName, int fieldType ) {
    rowMeta.addValueMeta( getValueMetaInterface( fieldName, fieldType ) );
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
        return new ValueMetaTimestamp( fieldName );
      case ValueMetaInterface.TYPE_DATE:
        return new ValueMetaDate( fieldName );
      case ValueMetaInterface.TYPE_BOOLEAN:
        return new ValueMetaBoolean( fieldName );
      case ValueMetaInterface.TYPE_BINARY:
        return new ValueMetaBinary( fieldName );
    }
    return null;
  }
}
