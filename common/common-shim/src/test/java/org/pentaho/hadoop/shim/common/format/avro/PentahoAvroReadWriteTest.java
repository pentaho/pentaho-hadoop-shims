package org.pentaho.hadoop.shim.common.format.avro;

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
import org.pentaho.hadoop.shim.api.format.AvroSpec;
import org.pentaho.hadoop.shim.api.format.IPentahoAvroOutputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcOutputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;
import org.pentaho.hadoop.shim.common.format.orc.PentahoOrcInputFormat;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PentahoAvroReadWriteTest {
  private InetAddress inetAddress1;

  {
    try {
      inetAddress1 = InetAddress.getByName( "www.pentaho.com" );
    } catch ( Exception e ){
      //should not happen
    }
  }

  private final String[][] schemaData = new String[][] {
    { "avroField1", "pentahoField1", String.valueOf( AvroSpec.DataType.STRING.ordinal() ), String.valueOf( ValueMetaInterface.TYPE_STRING ), "default" },
    { "avroField2", "pentahoField2", String.valueOf( AvroSpec.DataType.STRING.ordinal() ), String.valueOf( ValueMetaInterface.TYPE_STRING ), "default2" },
    { "avroDouble3", "pentahoNumber3", String.valueOf( AvroSpec.DataType.DOUBLE.ordinal() ), String.valueOf( ValueMetaInterface.TYPE_NUMBER ), "1234.0" },
    { "avroDecimal4", "pentahoBigNumber4", String.valueOf( AvroSpec.DataType.DECIMAL.ordinal() ), String.valueOf( ValueMetaInterface.TYPE_BIGNUMBER ), "123456789.98765" },
    { "avroString5", "pentahoInet5", String.valueOf( AvroSpec.DataType.STRING.ordinal() ), String.valueOf( ValueMetaInterface.TYPE_INET ), inetAddress1.getHostAddress() },
    { "avroBoolean6", "pentahoBoolean6", String.valueOf( AvroSpec.DataType.BOOLEAN.ordinal() ), String.valueOf( ValueMetaInterface.TYPE_BOOLEAN ), "true" },
    { "avroInt7", "pentahoInt7", String.valueOf( AvroSpec.DataType.INTEGER.ordinal() ), String.valueOf( ValueMetaInterface.TYPE_INTEGER ), "-33456" },
    { "avroDate8", "pentahoDate8", String.valueOf( AvroSpec.DataType.DATE.ordinal() ), String.valueOf( ValueMetaInterface.TYPE_DATE ), "1980/01/01 00:00:00.000" },
    { "avroTimestamp9", "pentahoTimestamp9", String.valueOf( AvroSpec.DataType.TIMESTAMP_MILLIS.ordinal() ), String.valueOf( ValueMetaInterface.TYPE_TIMESTAMP ), "1980/01/01 00:00:00.000" },
    { "avroBytes10", "pentahoBinary10", String.valueOf( AvroSpec.DataType.BYTES.ordinal() ), String.valueOf( ValueMetaInterface.TYPE_BINARY ), "binary" }
  };
  
  private final int AVRO_NAME_INDEX = 0;
  private final int PENTAHO_NAME_INDEX = 1;
  private final int AVRO_TYPE_INDEX = 2;
  private final int PDI_TYPE_INDEX = 3;
  private final int DEFAULT_VALUE_INDEX = 4;

  public TemporaryFolder tempFolder = new TemporaryFolder();
  PentahoAvroOutputFormat avroOutputFormat = null;
  private RowMeta rowMeta;
  private List<AvroOutputField> avroOutputFields = null;
  private List<AvroInputField> avroInputFields = null;
  private DateFormat dateFormat = new SimpleDateFormat( "yyyy/MM/dd HH:mm:ss.SSS" );
  private Object[][] rowData;
  private String filePath;

  @Before
  public void setup() throws Exception {
    tempFolder.create();
    avroOutputFormat = new PentahoAvroOutputFormat();
    avroOutputFormat.setNameSpace( "nameSpace" );
    avroOutputFormat.setRecordName( "recordName" );


    // Set up the Avro Schema Description and rowMeta for the first set of fields.
    avroOutputFields = new ArrayList<AvroOutputField>();
    avroInputFields = new ArrayList<AvroInputField>();
    rowMeta = new RowMeta();
    for ( String[] schemaField : schemaData ) {
      AvroInputField avroInputField = new AvroInputField();
      avroInputField.setAvroFieldName( schemaField[AVRO_NAME_INDEX] );
      avroInputField.setPentahoFieldName( schemaField[ PENTAHO_NAME_INDEX ] );
      avroInputField.setAvroType( AvroSpec.DataType.values()[Integer.parseInt( schemaField[AVRO_TYPE_INDEX] )] );
      avroInputField.setPentahoType( Integer.valueOf( schemaField[PDI_TYPE_INDEX] ) );
      avroInputFields.add( avroInputField );

      AvroOutputField avroOutputField = new AvroOutputField();
      avroOutputField.setAvroFieldName( schemaField[AVRO_NAME_INDEX] );
      avroOutputField.setPentahoFieldName( schemaField[ PENTAHO_NAME_INDEX ] );
      avroOutputField.setAvroType( AvroSpec.DataType.values()[Integer.parseInt( schemaField[AVRO_TYPE_INDEX] )] );
      avroOutputField.setAllowNull( true );
      avroOutputField.setDefaultValue( null );
      avroOutputFields.add( avroOutputField );
      addFieldToRowMeta( schemaField[ PENTAHO_NAME_INDEX ], Integer.valueOf( schemaField[PDI_TYPE_INDEX] ) );
    }

    // Set up the Avro Schema Description and rowMeta for the second set of fields.
    for ( String[] schemaField : schemaData ) {
      AvroInputField avroInputField = new AvroInputField();
      avroInputField.setAvroFieldName( schemaField[AVRO_NAME_INDEX] + "0" );
      avroInputField.setPentahoFieldName( schemaField[ PENTAHO_NAME_INDEX ] + "0" );
      avroInputField.setAvroType( AvroSpec.DataType.values()[Integer.parseInt( schemaField[AVRO_TYPE_INDEX] )] );
      avroInputField.setPentahoType( Integer.valueOf( schemaField[PDI_TYPE_INDEX] ) );
      avroInputFields.add( avroInputField );

      AvroOutputField avroOutputField = new AvroOutputField();
      avroOutputField.setAvroFieldName( schemaField[AVRO_NAME_INDEX] + "0");
      avroOutputField.setPentahoFieldName( schemaField[ PENTAHO_NAME_INDEX ] + "0");
      avroOutputField.setAvroType( AvroSpec.DataType.values()[Integer.parseInt( schemaField[AVRO_TYPE_INDEX] )] );
      avroOutputField.setAllowNull( true );
      avroOutputField.setDefaultValue( schemaField[ DEFAULT_VALUE_INDEX ] );
      avroOutputFields.add( avroOutputField );
      addFieldToRowMeta( schemaField[ PENTAHO_NAME_INDEX ] + "0", Integer.valueOf( schemaField[PDI_TYPE_INDEX] ) );
    }

    // Set up the output fields.
    avroOutputFormat.setFields( avroOutputFields );

    Date date1 = ( dateFormat.parse( "2001/11/01 00:00:00.000" ) );
    Date date2 = ( dateFormat.parse( "1999/12/31 00:00:00.000" ) );
    Date timeStamp1 = new Timestamp( dateFormat.parse( "2001/11/01 20:30:15.123" ).getTime() );
    Date timeStamp2 = new Timestamp( dateFormat.parse( "1999/12/31 23:59:59.999" ).getTime() );

    // Populate three rows of values.  These are values we will write rows with.  Whe we read back the rows from the
    // file they will be compared with these values to know it made the round trip.
    rowData = new Object[][] {
//      { "Row1Field1", "Row1Field2", 3.1, new BigDecimal( 4.1, MathContext.DECIMAL64 ),
//      InetAddress.getByName( "www.microsoft.com" ), true, 1L, date1, timeStamp1, "foobar".getBytes(),
//      "Row1Field3", "Row1Field4", 3.1, new BigDecimal( 5.1, MathContext.DECIMAL64 ),
//      InetAddress.getByName( "www.microsoft.com" ), true, 2L, date1, timeStamp2, "foobar".getBytes()
//      },
//
//      { "Row2Field1", "Row2Field2", -3.2, new BigDecimal( -4.2, MathContext.DECIMAL64 ),
//        InetAddress.getByName( "www.microsoft.com" ), false, -2L, date2, timeStamp2, "Donald Duck".getBytes(),
//        "Row2Field3", "Row2Field4", -3.2, new BigDecimal( -4.2, MathContext.DECIMAL64 ),
//        InetAddress.getByName( "www.microsoft.com" ), false, -3L, date2, timeStamp2, "Donald Duck".getBytes()
//      },

      { "Row3Field1", null, null, null, null, null, null, null, null, null,
        "Row3Field4", null, null, null, null, null, null, null, null, null
      }
    };
  }

  @Test
  public void testAvroFileWriteAndRead() throws Exception {
    doReadWrite( IPentahoAvroOutputFormat.COMPRESSION.UNCOMPRESSED, "avroOutputNone.avro", false );
    doReadWrite( IPentahoAvroOutputFormat.COMPRESSION.SNAPPY, "avroOutputSnappy.avro", false );
    doReadWrite( IPentahoAvroOutputFormat.COMPRESSION.DEFLATE, "avroOutputDeflate.avro", false );
  }

  @Test(expected = FileAlreadyExistsException.class)
  public void testOverwriteFileIsFalse() throws Exception {
    doReadWrite( IPentahoAvroOutputFormat.COMPRESSION.UNCOMPRESSED, "avroOutputNone.orc", false );
    doReadWrite( IPentahoAvroOutputFormat.COMPRESSION.UNCOMPRESSED, "avroOutputNone.orc", false );
  }

  @Test
  public void testOverwriteFileIsTrue() throws Exception {
    doReadWrite( IPentahoAvroOutputFormat.COMPRESSION.UNCOMPRESSED, "avroOutputNone.orc", false );
    doReadWrite( IPentahoAvroOutputFormat.COMPRESSION.UNCOMPRESSED, "avroOutputNone.orc", true );
  }

  private void doReadWrite( IPentahoAvroOutputFormat.COMPRESSION compressionType, String outputFileName, boolean overwriteFile )
    throws Exception {
    avroOutputFormat.setCompression( compressionType );
    if (tempFolder.getRoot().toString().substring( 1, 2 ).equals( ":" )) {
      filePath = tempFolder.getRoot().toString().substring( 2 ) + "/" + outputFileName;
    } else {
      filePath = tempFolder.getRoot().toString() + "/" + outputFileName;
    }
    filePath = filePath.replace( "\\", "/" );

    try {
      avroOutputFormat.setOutputFile( filePath );
    } catch ( FileAlreadyExistsException e) {
      throw e;
    } catch ( Exception e ) {
      e.printStackTrace();
    }
    testRecordWriter();
    testRecordReader();
  }


  private void testRecordWriter() throws Exception {
    IPentahoOutputFormat.IPentahoRecordWriter avroRecordWriter = avroOutputFormat.createRecordWriter();
    Assert.assertNotNull( avroRecordWriter, "avroRecordWriter should NOT be null!" );
    Assert.assertTrue( avroRecordWriter instanceof PentahoAvroRecordWriter, "avroRecordWriter should be instance of PentahoAvroRecordWriter" );

    for ( int i = 0; i < rowData.length; i++ ) {
      avroRecordWriter.write( new RowMetaAndData( rowMeta, rowData[ i ] ) );
    }
    try {
      avroRecordWriter.close();
    } catch ( Exception e ) {
      e.printStackTrace();
    }
  }

  private void testRecordReader() throws Exception {
    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.init( true );

    PentahoAvroInputFormat pentahoAvroInputFormat = new PentahoAvroInputFormat();
    pentahoAvroInputFormat.setInputFields( avroInputFields );
    pentahoAvroInputFormat.setInputFile( filePath );
    IPentahoInputFormat.IPentahoRecordReader pentahoRecordReader = pentahoAvroInputFormat.createRecordReader( null );
    final AtomicInteger rowNumber = new AtomicInteger();
    for ( RowMetaAndData row : pentahoRecordReader ) {
      final AtomicInteger fieldNumber = new AtomicInteger();
      avroInputFields.forEach( field -> testValue( field, row, rowNumber, fieldNumber ) );
      rowNumber.incrementAndGet();
    }
  }


  private void testValue( AvroInputField field, RowMetaAndData row, AtomicInteger rowNumber,
                          AtomicInteger fieldNumber ) {
    int fldNum = fieldNumber.getAndIncrement();
    int rowNum = rowNumber.get();
    Object origValue = rowData[ rowNum ][ fldNum ];
    Object readValue = row.getData()[ fldNum ];
    String errMsg = "field " + fldNum + " does not match in " + row;

    AvroOutputField outputField = null;
    boolean allowNull = true;
    String avroFieldName = field.getAvroFieldName();
    for (AvroOutputField tmpOutputField : avroOutputFields) {
      if (tmpOutputField.getAvroFieldName().equals( avroFieldName )) {
        outputField = tmpOutputField;
      }
    }

    if ( origValue == null && !outputField.getAllowNull() ) {
      //If here we are comparing the read value to the default value
      if ( field.getPentahoType() == ValueMetaInterface.TYPE_INET ) {
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
      if ( origValue instanceof BigDecimal ) {
        assert ( ( (BigDecimal) origValue ).compareTo(
          (BigDecimal) readValue ) == 0 );
      } else if ( origValue instanceof byte[] ) {
        assertEquals( errMsg, new String( (byte[]) origValue ),
          new String( (byte[]) readValue ) );
      } else if ( origValue instanceof InetAddress ) {
        byte[] origAddress = ( (InetAddress)origValue).getAddress();
        byte[] readAddress = ((InetAddress)readValue).getAddress();
        assertEquals( errMsg, new String( origAddress ) , new String( readAddress ) );
      } else {
        assertEquals( errMsg, origValue, readValue );
      }
    }
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
        ValueMetaTimestamp valueMetaTimestamp = new ValueMetaTimestamp( fieldName );
        valueMetaTimestamp.setConversionMask( "yyyy/MM/dd HH:mm:ss.SSS"  );
        return valueMetaTimestamp;
      case ValueMetaInterface.TYPE_DATE:
        ValueMetaDate valueMetaDate = new ValueMetaDate( fieldName );
        valueMetaDate.setConversionMask( "yyyy/MM/dd HH:mm:ss.SSS"  );
        return valueMetaDate;
      case ValueMetaInterface.TYPE_BOOLEAN:
        return new ValueMetaBoolean( fieldName );
      case ValueMetaInterface.TYPE_BINARY:
        return new ValueMetaBinary( fieldName );
    }
    return null;
  }

}
