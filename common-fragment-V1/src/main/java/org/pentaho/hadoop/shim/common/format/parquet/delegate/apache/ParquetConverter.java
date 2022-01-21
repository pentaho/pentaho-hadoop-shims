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
package org.pentaho.hadoop.shim.common.format.parquet.delegate.apache;


import org.apache.logging.log4j.LogManager;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.apache.logging.log4j.Logger;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.plugins.IValueMetaConverter;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.row.value.ValueMetaBigNumber;
import org.pentaho.di.core.row.value.ValueMetaBinary;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaConversionException;
import org.pentaho.di.core.row.value.ValueMetaConverter;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaInternetAddress;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.row.value.ValueMetaTimestamp;
import org.pentaho.hadoop.shim.api.format.IParquetInputField;
import org.pentaho.hadoop.shim.api.format.ParquetSpec;
import org.pentaho.hadoop.shim.common.format.parquet.ParquetInputField;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.lang.Math.pow;

/**
 * Converter for read/write Hitachi Vantara row from/into Parquet files.
 * <p>
 * TYPE_DATE and TYPE_TIMESTAMP should be processed via Parquet's TIMESTAMP_MILLIS because Kettle's Date supports time
 * also. TIMESTAMP_MICROS is defined not in all Parquet implementations.
 *
 * @author Alexander Buloichik
 */
public class ParquetConverter {
  public static final int PARQUET_JOB_ID = Integer.MAX_VALUE;
  public static final String PARQUET_SCHEMA_CONF_KEY = "PentahoParquetSchema";
  private final List<? extends IParquetInputField> inputFields;

  public ParquetConverter( List<? extends IParquetInputField> inputFields ) {
    this.inputFields = inputFields;
  }

  public static List<IParquetInputField> buildInputFields( MessageType schema ) {
    List<IParquetInputField> inputFields = new ArrayList<>();

    for ( Type type : schema.getFields() ) {
      if ( type.isPrimitive() ) {
        inputFields.add( convertField( type ) );
      }
    }

    return inputFields;
  }

  private static IParquetInputField convertField( Type t ) {
    OriginalType originalType = t.getOriginalType();
    ParquetSpec.DataType dataType = null;
    int scale = 0;
    int precision = 0;

    switch ( t.asPrimitiveType().getPrimitiveTypeName() ) {
      case BINARY:
        if ( originalType == null ) {
          dataType = ParquetSpec.DataType.BINARY;
          break;
        }

        switch ( originalType ) {
          case DECIMAL:
            dataType = ParquetSpec.DataType.DECIMAL;
            precision = t.asPrimitiveType().getDecimalMetadata().getPrecision();
            scale = t.asPrimitiveType().getDecimalMetadata().getScale();
            break;
          case UTF8:
            dataType = ParquetSpec.DataType.UTF8;
            break;
          case ENUM:
            dataType = ParquetSpec.DataType.ENUM;
            break;
          default:
            dataType = ParquetSpec.DataType.BINARY;
        }
        break;
      case BOOLEAN:
        dataType = ParquetSpec.DataType.BOOLEAN;
        break;
      case DOUBLE:
        dataType = ParquetSpec.DataType.DOUBLE;
        break;
      case FLOAT:
        dataType = ParquetSpec.DataType.FLOAT;
        break;
      case INT32:
        if ( originalType == null ) {
          dataType = ParquetSpec.DataType.INT_32;
          break;
        }

        switch ( originalType ) {
          case DECIMAL:
            dataType = ParquetSpec.DataType.DECIMAL_INT_32;
            precision = t.asPrimitiveType().getDecimalMetadata().getPrecision();
            scale = t.asPrimitiveType().getDecimalMetadata().getScale();
            break;
          case DATE:
            dataType = ParquetSpec.DataType.DATE;
            break;
          case INT_8:
            dataType = ParquetSpec.DataType.INT_8;
            break;
          case INT_16:
            dataType = ParquetSpec.DataType.INT_8;
            break;
          case INT_32:
            dataType = ParquetSpec.DataType.INT_32;
            break;
          case UINT_8:
            dataType = ParquetSpec.DataType.UINT_8;
            break;
          case UINT_16:
            dataType = ParquetSpec.DataType.UINT_16;
            break;
          case UINT_32:
            dataType = ParquetSpec.DataType.UINT_32;
            break;
          case TIME_MILLIS:
            dataType = ParquetSpec.DataType.TIME_MILLIS;
            break;
          default:
            dataType = ParquetSpec.DataType.INT_32;
        }
        break;
      case INT64:
        if ( originalType == null ) {
          dataType = ParquetSpec.DataType.INT_64;
          break;
        }

        switch ( originalType ) {
          case DECIMAL:
            dataType = ParquetSpec.DataType.DECIMAL_INT_64;
            precision = t.asPrimitiveType().getDecimalMetadata().getPrecision();
            scale = t.asPrimitiveType().getDecimalMetadata().getScale();
            break;
          case TIMESTAMP_MILLIS:
            dataType = ParquetSpec.DataType.TIMESTAMP_MILLIS;
            break;
          default:
            dataType = ParquetSpec.DataType.INT_64;
        }
        break;
      case INT96:
        dataType = ParquetSpec.DataType.INT_96;
        break;
      case FIXED_LEN_BYTE_ARRAY:
        if ( originalType == null ) {
          dataType = ParquetSpec.DataType.FIXED_LEN_BYTE_ARRAY;
          break;
        }

        switch ( originalType ) {
          case DECIMAL:
            dataType = ParquetSpec.DataType.DECIMAL_FIXED_LEN_BYTE_ARRAY;
            precision = t.asPrimitiveType().getDecimalMetadata().getPrecision();
            scale = t.asPrimitiveType().getDecimalMetadata().getScale();
            break;
          default:
            dataType = ParquetSpec.DataType.FIXED_LEN_BYTE_ARRAY;
        }
        break;
      default:
        dataType = ParquetSpec.DataType.NULL;
    }

    ParquetInputField field = new ParquetInputField();
    field.setPentahoFieldName( t.getName() );
    field.setFormatFieldName( t.getName() );
    field.setPentahoType( dataType.getPdiType() );
    field.setParquetType( dataType );
    field.setPrecision( precision );
    field.setScale( scale );
    return field;
  }

  public static class MyRecordMaterializer extends RecordMaterializer<RowMetaAndData> {
    private final MyGroupConverter root;

    public MyRecordMaterializer( ParquetConverter converter ) {
      root = new MyGroupConverter( converter );
    }

    @Override
    public RowMetaAndData getCurrentRecord() {
      return root.getCurrentRecord();
    }

    @Override
    public GroupConverter getRootConverter() {
      return root;
    }
  }

  public static class MyGroupConverter extends GroupConverter {
    private final RowMeta fields = new RowMeta();
    protected RowMetaAndData current;
    private final Converter[] converters;
    private int count;
    private final IValueMetaConverter valueMetaConverter = new ValueMetaConverter();
    private static final Logger logger = LogManager.getLogger( MyGroupConverter.class );

    private Object convertFromSourceToTargetType( IValueMetaConverter valueMetaConverter, Object stagingValue,
                                                  IParquetInputField f ) {
      try {
        String dateFormatStr = f.getStringFormat();
        if ( ( dateFormatStr == null ) || ( dateFormatStr.trim().length() == 0 ) ) {
          dateFormatStr = ValueMetaBase.DEFAULT_DATE_FORMAT_MASK;
        }
        valueMetaConverter.setDatePattern( new SimpleDateFormat( dateFormatStr ) );

        return valueMetaConverter.convertFromSourceToTargetDataType(
          f.getParquetType().getPdiType(), f.getPentahoType(), stagingValue );
      } catch ( ValueMetaConversionException e ) {
        logger.error( e );
        return null;
      }
    }

    private void addValueMeta( int pdiType, String pentahoFieldName ) {
      switch ( pdiType ) {
        case ValueMetaInterface.TYPE_BINARY:
          fields.addValueMeta( new ValueMetaBinary( pentahoFieldName ) );
          break;
        case ValueMetaInterface.TYPE_BIGNUMBER:
          fields.addValueMeta( new ValueMetaBigNumber( pentahoFieldName ) );
          break;
        case ValueMetaInterface.TYPE_BOOLEAN:
          fields.addValueMeta( new ValueMetaBoolean( pentahoFieldName ) );
          break;
        case ValueMetaInterface.TYPE_DATE:
          fields.addValueMeta( new ValueMetaDate( pentahoFieldName ) );
          break;
        case ValueMetaInterface.TYPE_INET:
          fields.addValueMeta( new ValueMetaInternetAddress( pentahoFieldName ) );
          break;
        case ValueMetaInterface.TYPE_INTEGER:
          fields.addValueMeta( new ValueMetaInteger( pentahoFieldName ) );
          break;
        case ValueMetaInterface.TYPE_NUMBER:
          fields.addValueMeta( new ValueMetaNumber( pentahoFieldName ) );
          break;
        case ValueMetaInterface.TYPE_STRING:
          fields.addValueMeta( new ValueMetaString( pentahoFieldName ) );
          break;
        case ValueMetaInterface.TYPE_TIMESTAMP:
          fields.addValueMeta( new ValueMetaTimestamp( pentahoFieldName ) );
          break;
      }
    }

    public MyGroupConverter( ParquetConverter converter ) {
      count = 0;
      for ( IParquetInputField f : converter.inputFields ) {
        if ( f.getFormatFieldName() != null ) {
          count++;
        }
      }
      converters = new Converter[ count ];
      int i = 0;
      for ( IParquetInputField f : converter.inputFields ) {
        if ( f.getFormatFieldName() == null ) {
          continue;
        }

        final int index = i;
        addValueMeta( f.getPentahoType(), f.getPentahoFieldName() );

        switch ( f.getParquetType().getPdiType() ) {
          case ValueMetaInterface.TYPE_NUMBER:
            converters[ i ] = new PrimitiveConverter() {
              @Override
              public void addDouble( double value ) {
                current.getData()[ index ] = value;
                current.getData()[ index ] =
                  convertFromSourceToTargetType( valueMetaConverter, current.getData()[ index ], f );
                updateValueMeta( index, f );
              }

              @Override
              public void addFloat( float value ) {
                current.getData()[ index ] = new BigDecimal( String.valueOf( value ) ).doubleValue();
                current.getData()[ index ] =
                  convertFromSourceToTargetType( valueMetaConverter, current.getData()[ index ], f );
                updateValueMeta( index, f );
              }
            };
            break;
          case ValueMetaInterface.TYPE_INTEGER:
            converters[ i ] = new PrimitiveConverter() {
              @Override
              public void addInt( int value ) {
                current.getData()[ index ] = (long) value;
                current.getData()[ index ] =
                  convertFromSourceToTargetType( valueMetaConverter, current.getData()[ index ], f );
              }

              @Override
              public void addLong( long value ) {
                current.getData()[ index ] = value;
                current.getData()[ index ] =
                  convertFromSourceToTargetType( valueMetaConverter, current.getData()[ index ], f );
                updateValueMeta( index, f );
              }
            };
            break;
          case ValueMetaInterface.TYPE_BIGNUMBER:
            converters[ i ] = new PrimitiveConverter() {
              @Override
              public void addBinary( Binary value ) {
                current.getData()[ index ] = binaryToDecimal( value, f.getPrecision(), f.getScale() );
                current.getData()[ index ] =
                  convertFromSourceToTargetType( valueMetaConverter, current.getData()[ index ], f );
                updateValueMeta( index, f );
              }

              @Override
              public void addInt( int value ) {
                current.getData()[ index ] = new BigDecimal( BigInteger.valueOf( value ), f.getScale() );
                current.getData()[ index ] =
                  convertFromSourceToTargetType( valueMetaConverter, current.getData()[ index ], f );
                updateValueMeta( index, f );
              }

              @Override
              public void addLong( long value ) {
                current.getData()[ index ] = new BigDecimal( BigInteger.valueOf( value ), f.getScale() );
                current.getData()[ index ] =
                  convertFromSourceToTargetType( valueMetaConverter, current.getData()[ index ], f );
                updateValueMeta( index, f );
              }
            };
            break;
          case ValueMetaInterface.TYPE_STRING:
            converters[ i ] = new PrimitiveConverter() {
              @Override
              public void addBinary( Binary value ) {
                current.getData()[ index ] = value.toStringUsingUTF8();
                current.getData()[ index ] =
                  convertFromSourceToTargetType( valueMetaConverter, current.getData()[ index ], f );
                updateValueMeta( index, f );
              }
            };
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            converters[ i ] = new PrimitiveConverter() {
              @Override
              public void addBoolean( boolean value ) {
                current.getData()[ index ] = value;
                current.getData()[ index ] =
                  convertFromSourceToTargetType( valueMetaConverter, current.getData()[ index ], f );
                updateValueMeta( index, f );
              }
            };
            break;
          case ValueMetaInterface.TYPE_SERIALIZABLE:
            converters[ i ] = new PrimitiveConverter() {
              @Override
              public void addBinary( Binary value ) {
                current.getData()[ index ] = value.getBytes();
                current.getData()[ index ] =
                  convertFromSourceToTargetType( valueMetaConverter, current.getData()[ index ], f );
                updateValueMeta( index, f );
              }
            };
            break;
          case ValueMetaInterface.TYPE_BINARY:
            converters[ i ] = new PrimitiveConverter() {
              @Override
              public void addBinary( Binary value ) {
                if ( f.getPentahoType() == ValueMetaBase.TYPE_STRING ) {
                  current.getData()[ index ] = value.toStringUsingUTF8();
                } else {
                  current.getData()[ index ] = value.getBytes();
                  current.getData()[ index ] =
                    convertFromSourceToTargetType( valueMetaConverter, current.getData()[ index ], f );
                  updateValueMeta( index, f );
                }
              }
            };
            break;
          case ValueMetaInterface.TYPE_DATE:
            converters[ i ] = new PrimitiveConverter() {
              @Override
              public void addLong( long value ) {
                current.getData()[ index ] = new Date( value );
                current.getData()[ index ] =
                  convertFromSourceToTargetType( valueMetaConverter, current.getData()[ index ], f );
                updateValueMeta( index, f );
              }

              // the number of days from the Unix epoch, 1 January 1970.
              @Override
              public void addInt( int value ) {
                LocalDate localDate = LocalDate.ofEpochDay( 0 ).plusDays( value );
                current.getData()[ index ] = Date.from( localDate.atStartOfDay( ZoneId.systemDefault() ).toInstant() );
                current.getData()[ index ] =
                  convertFromSourceToTargetType( valueMetaConverter, current.getData()[ index ], f );
                updateValueMeta( index, f );
              }

              @Override
              public void addBinary( Binary value ) {
                current.getData()[ index ] = new Date( dateFromInt96( value ) );
                current.getData()[ index ] =
                  convertFromSourceToTargetType( valueMetaConverter, current.getData()[ index ], f );
                updateValueMeta( index, f );
              }
            };
            break;
          case ValueMetaInterface.TYPE_TIMESTAMP:
            converters[ i ] = new PrimitiveConverter() {
              @Override
              public void addLong( long value ) {
                current.getData()[ index ] = new Timestamp( value );
                current.getData()[ index ] =
                  convertFromSourceToTargetType( valueMetaConverter, current.getData()[ index ], f );
                updateValueMeta( index, f );
              }

              // the number of days from the Unix epoch, 1 January 1970.
              @Override
              public void addInt( int value ) {
                current.getData()[ index ] = new Timestamp( value * 24L * 60L * 60L * 1000L );
                current.getData()[ index ] =
                  convertFromSourceToTargetType( valueMetaConverter, current.getData()[ index ], f );
                updateValueMeta( index, f );
              }

              @Override
              public void addBinary( Binary value ) {
                current.getData()[ index ] = new Timestamp( dateFromInt96( value ) );
                current.getData()[ index ] =
                  convertFromSourceToTargetType( valueMetaConverter, current.getData()[ index ], f );
                updateValueMeta( index, f );
              }
            };
            break;
          case ValueMetaInterface.TYPE_INET:
            converters[ i ] = new PrimitiveConverter() {
              @Override
              public void addBinary( Binary value ) {
                try {
                  byte[] bytes = value.getBytes();
                  if ( bytes == null || bytes.length == 0 ) {
                    current.getData()[ index ] = null;
                  } else {
                    current.getData()[ index ] = InetAddress.getByAddress( bytes );
                    current.getData()[ index ] =
                      convertFromSourceToTargetType( valueMetaConverter, current.getData()[ index ], f );
                  }
                  updateValueMeta( index, f );
                } catch ( Exception ex ) {
                  throw new RuntimeException( ex );
                }
              }
            };
            break;
          default:
            throw new RuntimeException( "Undefined type: " + f.getPentahoFieldName() );
        }
        i++;
      }

    }

    private void updateValueMeta( int index, IParquetInputField inputField ) {
      String stringFormat = inputField.getStringFormat();
      if ( ( stringFormat != null ) && ( stringFormat.trim().length() > 0 ) ) {
        current.getValueMeta( index ).setConversionMask( stringFormat );
      }
    }

    private static long dateFromInt96( Binary value ) {
      byte[] readBuffer = value.getBytes();
      if ( readBuffer.length != 12 ) {
        throw new RuntimeException( "Invalid byte array length for INT96" );
      }

      long timeOfDayNanos =
        ( ( (long) readBuffer[ 7 ] << 56 ) + ( (long) ( readBuffer[ 6 ] & 255 ) << 48 )
          + ( (long) ( readBuffer[ 5 ] & 255 ) << 40 ) + ( (long) ( readBuffer[ 4 ] & 255 ) << 32 )
          + ( (long) ( readBuffer[ 3 ] & 255 ) << 24 ) + ( ( readBuffer[ 2 ] & 255 ) << 16 )
          + ( ( readBuffer[ 1 ] & 255 ) << 8 ) + ( readBuffer[ 0 ] & 255 ) );

      int julianDay =
        ( (int) ( readBuffer[ 11 ] & 255 ) << 24 ) + ( ( readBuffer[ 10 ] & 255 ) << 16 )
          + ( ( readBuffer[ 9 ] & 255 ) << 8 ) + ( readBuffer[ 8 ] & 255 );

      return ( julianDay - ParquetSpec.JULIAN_DAY_OF_EPOCH ) * 24L * 60L * 60L * 1000L + timeOfDayNanos / 1000000;
    }

    @Override
    public void start() {
      current = new RowMetaAndData( fields );
      current.setData( new Object[ count ] );
    }

    @Override
    public Converter getConverter( int fieldIndex ) {
      return converters[ fieldIndex ];
    }

    @Override
    public void end() {
    }


    public RowMetaAndData getCurrentRecord() {
      return current;
    }

    static BigDecimal binaryToDecimal( Binary value, int precision, int scale ) {
      /*
       * Precision <= 18 checks for the max number of digits for an unscaled long,
       * else treat with big integer conversion
       */
      if ( precision <= 18 ) {
        ByteBuffer buffer = value.toByteBuffer();
        byte[] bytes = buffer.array();
        int start = buffer.arrayOffset() + buffer.position();
        int end = buffer.arrayOffset() + buffer.limit();
        long unscaled = 0L;
        int i = start;
        while ( i < end ) {
          unscaled = ( unscaled << 8 | bytes[ i ] & 0xff );
          i++;
        }
        int bits = 8 * ( end - start );
        long unscaledNew = ( unscaled << ( 64 - bits ) ) >> ( 64 - bits );
        if ( unscaledNew <= -pow( 10, 18 ) || unscaledNew >= pow( 10, 18 ) ) {
          return new BigDecimal( unscaledNew );
        } else {
          return BigDecimal.valueOf( unscaledNew / pow( 10, scale ) );
        }
      } else {
        return new BigDecimal( new BigInteger( value.getBytes() ), scale );
      }
    }
  }
}
