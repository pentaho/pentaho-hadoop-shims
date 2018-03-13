/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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
import org.pentaho.di.core.row.value.ValueMetaConversionException;
import org.pentaho.di.core.row.value.ValueMetaConverter;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaInternetAddress;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaPluginType;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.row.value.ValueMetaTimestamp;
import org.pentaho.di.core.util.Assert;
import org.pentaho.hadoop.shim.api.format.IOrcInputField;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcOutputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat;
import org.pentaho.hadoop.shim.api.format.OrcSpec;

import java.math.BigDecimal;
import java.math.MathContext;
import java.net.InetAddress;
import java.nio.file.FileAlreadyExistsException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;


/**
 * Created by tkafalas on 11/3/2017.
 */
public class PentahoOrcReadWriteTest {
  private List<? extends IOrcInputField> orcInputFields;
  private RowMeta rowMeta;
  private Object[][] rowData;
  private PentahoOrcOutputFormat orcOutputFormat;
  private String filePath;
  private DateFormat dateFormat = new SimpleDateFormat( "yyyy/MM/dd HH:mm:ss.SSS" );
  private InetAddress inetAddress1;
  private List<OrcOutputField> fields;
  private String[][] fieldData;
  private ValueMetaConverter valueMetaConverter = new ValueMetaConverter();

  {
    try {
      inetAddress1 = InetAddress.getByName( "www.pentaho.com" );
    } catch ( Exception e ) {
      //should not happen
    }
  }

  private final int ORC_NAME_INDEX = 0;
  private final int PENTAHO_NAME_INDEX = 1;
  private final int ORC_TYPE_INDEX = 2;
  private final int PENTAHO_TYPE_INDEX = 3;
  private final int DEFAULT_VALUE_INDEX = 4;

  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setup() throws Exception {
    tempFolder.create();
    orcOutputFormat = new PentahoOrcOutputFormat();

    // Set up the Orc Schema Description and rowMeta for the first set of fields.
    orcInputFields = new ArrayList<IOrcInputField>();
    fields = new ArrayList<>();
    rowMeta = new RowMeta();

    Date date1 = ( dateFormat.parse( "2001/11/01 00:00:00.000" ) );
    Date date2 = ( dateFormat.parse( "1999/12/31 00:00:00.000" ) );
    Date timeStamp1 = new Timestamp( dateFormat.parse( "2001/11/01 20:30:15.123" ).getTime() );
    Date timeStamp2 = new Timestamp( dateFormat.parse( "1999/12/31 23:59:59.999" ).getTime() );

    //This is the metadata and default values we will use to test with.  We'll create a file with two sets of these
    // 10 fields.  The first set will allows nulls, the seconds set will not allow nulls and use the default values.
    // The second set of fields will have "def" appended to all field names to keep them unique.
    fieldData = new String[][] {
      { "orcField1", "pentahoField1", String.valueOf( OrcSpec.DataType.STRING.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_STRING ), "default" },
      { "orcField2", "pentahoField2", String.valueOf( OrcSpec.DataType.STRING.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_STRING ), "default2" },
      { "orcDouble3", "pentahoNumber3", String.valueOf( OrcSpec.DataType.DOUBLE.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_NUMBER ), "1234.0" },
      { "orcDouble4", "pentahoBigNumber4", String.valueOf( OrcSpec.DataType.DECIMAL.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_BIGNUMBER ), "123456789.98765" },
      { "orcBytes5", "pentahoInet5", String.valueOf( OrcSpec.DataType.STRING.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_INET ), inetAddress1.getHostAddress() },
      { "orcLong6", "pentahoBoolean6", String.valueOf( OrcSpec.DataType.BOOLEAN.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_BOOLEAN ), "true" },
      { "orcInt7", "pentahoInt7", String.valueOf( OrcSpec.DataType.INTEGER.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_INTEGER ), "-33456" },
      { "orcDate8", "pentahoDate8", String.valueOf( OrcSpec.DataType.DATE.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_DATE ), "1980/01/01 00:00:00.000" },
      //Conversion Tests
      { "orcTimestamp9", "pentahoTimestamp9", String.valueOf( OrcSpec.DataType.TIMESTAMP.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_TIMESTAMP ), "1980/01/01 00:00:00.000" },
      { "orcBytes10", "pentahoBinary10", String.valueOf( OrcSpec.DataType.BINARY.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_BINARY ), "binary" },
      { "orcStringToTinyInt11", "pentahoStringToTinyInt11", String.valueOf( OrcSpec.DataType.TINYINT.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_STRING ), "12" },
      { "orcStringToSmallInt12", "pentahoStringToSmallInt12", String.valueOf( OrcSpec.DataType.SMALLINT.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_STRING ), "61230" },
      { "orcStringToInt13", "pentahoStringToInt13", String.valueOf( OrcSpec.DataType.INTEGER.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_STRING ), "77777" },
      { "orcStringToFloat14", "pentahoStringToFloat14", String.valueOf( OrcSpec.DataType.FLOAT.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_STRING ), "1230.0123291015625" },
      { "orcStringToDouble15", "pentahoStringToDouble15", String.valueOf( OrcSpec.DataType.DOUBLE.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_STRING ), "1234.0123" },
      { "orcDateToString16", "pentahoDateToString16", String.valueOf( OrcSpec.DataType.STRING.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_DATE ), "1970/01/01 00:00:00.000" },
      { "orcIntToBigInt17", "pentahoIntToBigInt17", String.valueOf( OrcSpec.DataType.BIGINT.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_INTEGER ), "19577" },
      { "orcIntToFloat18", "pentahoIntToFloat18", String.valueOf( OrcSpec.DataType.FLOAT.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_INTEGER ), "19588.0" },
      { "orcIntToTinyInt19", "pentahoIntToTinyInt19", String.valueOf( OrcSpec.DataType.TINYINT.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_INTEGER ), "119" },
      { "orcIntToSmallInt20", "pentahoIntToSmallInt20", String.valueOf( OrcSpec.DataType.SMALLINT.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_INTEGER ), "-24586" },
      { "orcIntToDouble21", "pentahoIntToDouble21", String.valueOf( OrcSpec.DataType.DOUBLE.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_INTEGER ), "64586.0123" },
      { "orcDoubleToTinyInt22", "pentahoDoubleToTinyInt22", String.valueOf( OrcSpec.DataType.TINYINT.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_NUMBER ), "44" },
      { "orcDoubleToSmallInt23", "pentahoDoubleToSmallInt23", String.valueOf( OrcSpec.DataType.SMALLINT.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_NUMBER ), "956" },
      { "orcDoubleToInt24", "pentahoDoubleToInt24", String.valueOf( OrcSpec.DataType.INTEGER.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_NUMBER ), "4956" },
      { "orcDoubleToFloat25", "pentahoDoubleToFloat25", String.valueOf( OrcSpec.DataType.FLOAT.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_NUMBER ), "5306.1201171875" },
      { "orcTimestampToDate26", "pentahoTimestampToDate26", String.valueOf( OrcSpec.DataType.DATE.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_TIMESTAMP ), "1973/01/01 00:00:00.000" },
      { "orcTimestampToInt27", "pentahoTimestampToInt27", String.valueOf( OrcSpec.DataType.INTEGER.getId() ),
        String.valueOf( ValueMetaInterface.TYPE_TIMESTAMP ), "1234" },
    };

    // Populate three rows of values.  These are values we will write rows with.  Whe we read back the rows from the
    // file they will be compared with these values to know it made the round trip.
    rowData = new Object[][]
      { { "Row1Field1", "Row1Field2", 3.1, new BigDecimal( 4.1, MathContext.DECIMAL64 ),
        InetAddress.getByName( "www.microsoft.com" ), true, 1L, date1, timeStamp1, "foobar".getBytes(), "123",
        "64000", "88888", "7654.43212890625", "112345.330", date1, 23057L, 25612L, 106L, 15671L, 8532L, 44.0D, 6788.2D,
        2057.01D, 5068.537109375D, new Timestamp( date1.getTime() ), new Timestamp( date1.getTime() ),
        "Row1Field3", "Row1Field4", 3.1, new BigDecimal( 4.1, MathContext.DECIMAL64 ),
        InetAddress.getByName( "www.microsoft.com" ), true, 1L, date1, timeStamp2, "foobar".getBytes(), "123",
        "64000", "88888", "7654.43212890625", "112345.330", date1, 23057L, 25612L, 106L, 15671L, 8532L, 44.0D, 6788.2D,
        2057.01D, 5068.537109375D, new Timestamp( date1.getTime() ), new Timestamp( date1.getTime() )
      },

        { "Row2Field1", "Row2Field2", -3.2, new BigDecimal( -4.2, MathContext.DECIMAL64 ),
          InetAddress.getByName( "www.microsoft.com" ), false, -2L, date2, timeStamp2, "Donald Duck".getBytes(), "-124",
          "-64001", "-66666", "-5432.10888671875", "-26789.222", date2, -30572L, -62612L, -105L, -15607L, -7433L,
          102.0D, -457.3D, -5074.1D, -3278.2080078125D, new Timestamp( date2.getTime() ),
          new Timestamp( date2.getTime() ),
          "Row2Field3", "Row2Field4", -3.2, new BigDecimal( -4.2, MathContext.DECIMAL64 ),
          InetAddress.getByName( "www.microsoft.com" ), false, -2L, date2, timeStamp2, "Donald Duck".getBytes(), "-124",
          "-64001", "-66666", "-5432.10888671875", "-26789.222", date2, -30572L, -62612L, -105L, -15607L, -7433L,
          -102.0D, -457.3D, -5074.1D, -3278.2080078125D, new Timestamp( date2.getTime() ),
          new Timestamp( date2.getTime() )
        },

        { "Row3Field1", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null,
          "Row3Field4", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null }
      };

    for ( String[] schemaField : fieldData ) {
      OrcOutputField field = new OrcOutputField();
      field.setFormatFieldName( schemaField[ ORC_NAME_INDEX ] );
      field.setPentahoFieldName( schemaField[ PENTAHO_NAME_INDEX ] );
      field.setFormatType( Integer.valueOf( schemaField[ ORC_TYPE_INDEX ] ) );
      field.setPentahoType( Integer.valueOf( schemaField[ PENTAHO_TYPE_INDEX ] ) );
      field.setAllowNull( true );
      fields.add( field );
      addFieldToRowMeta( schemaField[ PENTAHO_NAME_INDEX ], Integer.valueOf( schemaField[ PENTAHO_TYPE_INDEX ] ) );
    }

    // Set up the Orc Schema Description and rowMeta for the second set of fields.
    for ( String[] schemaField : fieldData ) {
      OrcOutputField field = new OrcOutputField();
      field.setFormatFieldName( schemaField[ ORC_NAME_INDEX ] + "Def" );
      field.setPentahoFieldName( schemaField[ PENTAHO_NAME_INDEX ] + "Def" );
      field.setFormatType( Integer.valueOf( schemaField[ ORC_TYPE_INDEX ] ) );
      field.setPentahoType( Integer.valueOf( schemaField[ PENTAHO_TYPE_INDEX ] ) );
      field.setAllowNull( false );
      field.setDefaultValue( schemaField[ DEFAULT_VALUE_INDEX ] );
      fields.add( field );
      addFieldToRowMeta( schemaField[ PENTAHO_NAME_INDEX ] + "Def",
        Integer.valueOf( schemaField[ PENTAHO_TYPE_INDEX ] ) );
    }

    orcOutputFormat.setFields( fields );

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

  @Test( expected = FileAlreadyExistsException.class )
  public void testOverwriteFileIsFalse() throws Exception {
    doReadWrite( IPentahoOrcOutputFormat.COMPRESSION.NONE, "orcOutputNone.orc", false );
    doReadWrite( IPentahoOrcOutputFormat.COMPRESSION.NONE, "orcOutputNone.orc", false );
  }

  @Test
  public void testOverwriteFileIsTrue() throws Exception {
    doReadWrite( IPentahoOrcOutputFormat.COMPRESSION.NONE, "orcOutputNone.orc", false );
    doReadWrite( IPentahoOrcOutputFormat.COMPRESSION.NONE, "orcOutputNone.orc", true );
  }

  private void doReadWrite( IPentahoOrcOutputFormat.COMPRESSION compressionType, String outputFileName,
                            boolean overwriteFile )
    throws Exception {
    orcOutputFormat.setCompression( compressionType );
    if ( tempFolder.getRoot().toString().substring( 1, 2 ).equals( ":" ) ) {
      filePath = tempFolder.getRoot().toString().substring( 2 ) + "/" + outputFileName;
    } else {
      filePath = tempFolder.getRoot().toString() + "/" + outputFileName;
    }
    filePath = filePath.replace( "\\", "/" );

    try {
      orcOutputFormat.setOutputFile( filePath, overwriteFile );
    } catch ( FileAlreadyExistsException e ) {
      throw e;
    } catch ( Exception e ) {
      e.printStackTrace();
    }
    //Write the Orc File
    System.out.println( "Writing file " + filePath );
    testRecordWriter();
    //Test that we can extract the schema (TypeDescription) from the file
    System.out.println( "reading Schema " + filePath );
    testGetSchema();
    //Read it back and check values
    System.out.println( "Reading file " + filePath );
    testRecordReader();
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
    pentahoOrcInputFormat.setSchema( orcInputFields );
    pentahoOrcInputFormat.setInputFile( filePath );
    IPentahoInputFormat.IPentahoRecordReader pentahoRecordReader = pentahoOrcInputFormat.createRecordReader( null );
    final AtomicInteger rowNumber = new AtomicInteger();
    for ( RowMetaAndData row : pentahoRecordReader ) {
      final AtomicInteger fieldNumber = new AtomicInteger();
      orcInputFields.forEach( field -> testValue( field, row, rowNumber, fieldNumber ) );
      rowNumber.incrementAndGet();
    }
  }

  private void testGetSchema() throws Exception {
    PentahoOrcInputFormat pentahoOrcInputFormat = new PentahoOrcInputFormat();
    pentahoOrcInputFormat.setInputFile( filePath );

    assertNotNull( "Schema Description should be populated", orcInputFields );
    //If here we hopefully read the the TypeDescription out of the orc file
    orcInputFields = pentahoOrcInputFormat.readSchema();
    orcInputFields.forEach( field -> compareField( field, orcInputFields ) );
  }

  private void testValue( IOrcInputField field, RowMetaAndData row, AtomicInteger rowNumber,
                          AtomicInteger fieldNumber ) {
    int fldNum = fieldNumber.getAndIncrement();
    int rowNum = rowNumber.get();
    Object origValue = rowData[ rowNum ][ fldNum ];
    Object readValue = row.getData()[ fldNum ];
    OrcOutputField outputField = fields.get( fldNum );
    String errMsg = "field number " + fldNum + ", " + outputField.getFormatFieldName() + " does not match in " + row;

    if ( origValue == null && outputField.getAllowNull() == false ) {
      //If here we are comparing the read value to the default value
      if ( outputField.getPentahoType() == ValueMetaInterface.TYPE_INET ) {
        assertTrue( errMsg, readValue.toString().contains( outputField.getDefaultValue() ) );
      } else if ( field.getPentahoType() == ValueMetaInterface.TYPE_DATE ) {
        try {
          assertEquals( errMsg, dateFormat.parse( outputField.getDefaultValue() ).toString(), readValue.toString() );
        } catch ( ParseException e ) {
          e.printStackTrace();
        }
      } else if ( field.getPentahoType() == ValueMetaInterface.TYPE_TIMESTAMP ) {
        try {
          assertEquals( errMsg, new Timestamp( dateFormat.parse( outputField.getDefaultValue() ).getTime() ).toString(),
            readValue.toString() );
        } catch ( ParseException e ) {
          e.printStackTrace();
        }
      } else if ( field.getPentahoType() == ValueMetaInterface.TYPE_BINARY ) {
        assertEquals( errMsg, outputField.getDefaultValue(), new String( (byte[]) readValue ) );
      } else {
        assertEquals( errMsg, outputField.getDefaultValue(), readValue.toString() );
      }
    } else {
      // If here we are comparing read value with the original value
      if ( outputField.getPentahoType() != field.getPentahoType() ) {
        try {
          Object convertedOriginalValue = valueMetaConverter
            .convertFromSourceToTargetDataType( outputField.getPentahoType(), field.getPentahoType(), origValue );
          assertEquals( errMsg, convertedOriginalValue, readValue );
        } catch ( ValueMetaConversionException e ) {
          fail( e.getMessage() );
        }
      } else if ( origValue instanceof InetAddress ) {
        assertTrue( errMsg, origValue.toString().contains( (String) readValue ) );
      } else if ( origValue instanceof BigDecimal ) {
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

  private IOrcInputField findByOrcFieldName( String name, List<? extends IOrcInputField> inputFields ) {
    return inputFields.stream()
      .filter( x -> name.equals( x.getFormatFieldName() ) )
      .findFirst().orElse( null );
  }

  private void compareField( IOrcInputField field, List<? extends IOrcInputField> schemaDesc ) {
    IOrcInputField readField = findByOrcFieldName( field.getFormatFieldName(), schemaDesc );
    assertNotNull( "Field " + field.getFormatFieldName() + " should be found in the read schema", readField );
    assertEquals( "Field " + field.getFormatFieldName(), field.getPentahoType(), readField.getPentahoType() );
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
