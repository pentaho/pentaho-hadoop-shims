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
package org.pentaho.hadoop.shim.common.format.parquet;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

//#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI"
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
//#endif
//#if shim_type=="CDH" || shim_type=="MAPR"
//$import parquet.io.api.Binary;
//$import parquet.io.api.Converter;
//$import parquet.io.api.GroupConverter;
//$import parquet.io.api.PrimitiveConverter;
//$import parquet.io.api.RecordConsumer;
//$import parquet.io.api.RecordMaterializer;
//$import parquet.schema.MessageType;
//$import parquet.schema.OriginalType;
//$import parquet.schema.PrimitiveType;
//$import parquet.schema.PrimitiveType.PrimitiveTypeName;
//$import parquet.schema.Type;
//$import parquet.schema.Type.Repetition;
//#endif

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBigNumber;
import org.pentaho.di.core.row.value.ValueMetaBinary;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaSerializable;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.row.value.ValueMetaTimestamp;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

/**
 * Converter for read/write Hitachi Vantara row from/into Parquet files.
 * 
 * TYPE_DATE and TYPE_TIMESTAMP should be processed via Parquet's TIMESTAMP_MILLIS because Kettle's Date supports time
 * also. TIMESTAMP_MICROS is defined not in all Parquet implementations.
 *
 * @author Alexander Buloichik
 */
public class ParquetConverter {
  public static final int PARQUET_JOB_ID = Integer.MAX_VALUE;
  public static final String PARQUET_SCHEMA_CONF_KEY = "PentahoParquetSchema";

  private final SchemaDescription schema;

  public ParquetConverter( SchemaDescription schema ) {
    this.schema = schema;
  }

  public MessageType createParquetSchema() {
    List<Type> types = new ArrayList<>();

    schema.forEach( f -> types.add( convertField( f ) ) );

    if ( types.isEmpty() ) {
      throw new IllegalArgumentException( "Schema should contain at least one field" );
    }

    return new MessageType( "parquet-schema", types );
  }

  public static SchemaDescription createSchemaDescription( MessageType schema ) {
    SchemaDescription r = new SchemaDescription();

    schema.getFields().forEach( t -> r.addField( convertField( r, t ) ) );

    return r;
  }

  private static SchemaDescription.Field convertField( SchemaDescription schema, Type t ) {
    boolean allowNull = t.getRepetition() != Repetition.REQUIRED;
    switch ( t.asPrimitiveType().getPrimitiveTypeName() ) {
      case BINARY:
        return schema.new Field( t.getName(), t.getName(), ValueMetaInterface.TYPE_STRING, allowNull );
      case BOOLEAN:
        return schema.new Field( t.getName(), t.getName(), ValueMetaInterface.TYPE_BOOLEAN, allowNull );
      case DOUBLE:
      case FLOAT:
        return schema.new Field( t.getName(), t.getName(), ValueMetaInterface.TYPE_NUMBER, allowNull );
      case INT32:
      case INT64:
        if ( t.getOriginalType() == OriginalType.DATE || t.getOriginalType() == OriginalType.TIME_MILLIS
            || t.getOriginalType() == OriginalType.TIMESTAMP_MILLIS ) {
          return schema.new Field( t.getName(), t.getName(), ValueMetaInterface.TYPE_DATE, allowNull );
        } else {
          return schema.new Field( t.getName(), t.getName(), ValueMetaInterface.TYPE_INTEGER, allowNull );
        }
      case INT96:
        return schema.new Field( t.getName(), t.getName(), ValueMetaInterface.TYPE_DATE, allowNull );
      default:
        throw new RuntimeException( "Undefined type: " + t );
    }
  }

  private PrimitiveType convertField( SchemaDescription.Field f ) {
    Repetition rep = f.allowNull ? Repetition.OPTIONAL : Repetition.REQUIRED;
    switch ( f.pentahoValueMetaType ) {
      case ValueMetaInterface.TYPE_NUMBER:
        return new PrimitiveType( rep, PrimitiveTypeName.DOUBLE, f.formatFieldName );
      case ValueMetaInterface.TYPE_STRING:
        return new PrimitiveType( rep, PrimitiveTypeName.BINARY, f.formatFieldName, OriginalType.UTF8 );
      case ValueMetaInterface.TYPE_BOOLEAN:
        return new PrimitiveType( rep, PrimitiveTypeName.BOOLEAN, f.formatFieldName );
      case ValueMetaInterface.TYPE_INTEGER:
        return new PrimitiveType( rep, PrimitiveTypeName.INT64, f.formatFieldName, OriginalType.INT_64 );
      case ValueMetaInterface.TYPE_BIGNUMBER:
        return new PrimitiveType( rep, PrimitiveTypeName.DOUBLE, f.formatFieldName );
      case ValueMetaInterface.TYPE_SERIALIZABLE:
      case ValueMetaInterface.TYPE_BINARY:
        return new PrimitiveType( rep, PrimitiveTypeName.BINARY, f.formatFieldName );
      case ValueMetaInterface.TYPE_DATE:
      case ValueMetaInterface.TYPE_TIMESTAMP:
        return new PrimitiveType( rep, PrimitiveTypeName.INT64, f.formatFieldName, OriginalType.TIMESTAMP_MILLIS );
      default:
        throw new RuntimeException( "Undefined type: " + f.pentahoValueMetaType );
    }
  }

  private void writeField( SchemaDescription.Field field, int index, RowMetaAndData row, RecordConsumer consumer )
    throws KettleValueException {
    int fieldIndex = row.getRowMeta().indexOfValue( field.pentahoFieldName );
    if ( fieldIndex < 0 ) {
      if ( field.allowNull ) {
        return;
      } else {
        throw new KettleValueException( "Required field '" + field.pentahoFieldName + "' not found in rowset" );
      }
    }
    if ( row.isEmptyValue( field.pentahoFieldName ) ) {
      if ( field.allowNull ) {
        return;
      } else {
        if ( field.defaultValue == null ) {
          throw new KettleValueException(
              "Required field '" + field.pentahoFieldName + "' contains no data and default values not defined" );
        } else {
          // put default value
          consumer.startField( field.formatFieldName, index );
          switch ( field.pentahoValueMetaType ) {
            case ValueMetaInterface.TYPE_NUMBER:
              consumer.addDouble( Double.parseDouble( field.defaultValue ) );
              break;
            case ValueMetaInterface.TYPE_STRING:
              consumer.addBinary( Binary.fromString( field.defaultValue ) );
              break;
            case ValueMetaInterface.TYPE_BOOLEAN:
              consumer.addBoolean( Boolean.parseBoolean( field.defaultValue ) );
              break;
            case ValueMetaInterface.TYPE_INTEGER:
              consumer.addLong( Long.parseLong( field.defaultValue ) );
              break;
            case ValueMetaInterface.TYPE_BIGNUMBER:
              consumer.addDouble( Double.parseDouble( field.defaultValue ) );
              break;
            case ValueMetaInterface.TYPE_SERIALIZABLE:
              /**
               * 'fromByteArray' deprecated in the HDP, but CDH doesn't have 'fromReusedByteArray' yet.
               */
      //#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI"
              consumer.addBinary( Binary.fromReusedByteArray( new byte[0] ) );
      //#endif
      //$     consumer.addBinary( Binary.fromByteArray( row.getBinary( fieldIndex, new byte[0] ) ) );
      //#if shim_type=="CDH" || shim_type=="MAPR"
      //#endif
              break;
            case ValueMetaInterface.TYPE_BINARY:
              /**
               * 'fromByteArray' deprecated in the HDP, but CDH doesn't have 'fromReusedByteArray' yet.
               */
      //#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI"
              consumer.addBinary( Binary.fromReusedByteArray( new byte[0] ) );
      //#endif
      //$     consumer.addBinary( Binary.fromByteArray( row.getBinary( fieldIndex, new byte[0] ) ) );
      //#if shim_type=="CDH" || shim_type=="MAPR"
      //#endif
              break;
            case ValueMetaInterface.TYPE_DATE:
            case ValueMetaInterface.TYPE_TIMESTAMP:
              consumer.addLong( Long.parseLong( field.defaultValue ) );
              break;
            default:
              throw new RuntimeException( "Undefined type: " + field.pentahoValueMetaType );
          }
          consumer.endField( field.formatFieldName, index );
          return;
        }
      }
    }
    consumer.startField( field.formatFieldName, index );
    switch ( field.pentahoValueMetaType ) {
      case ValueMetaInterface.TYPE_NUMBER:
        consumer.addDouble( row.getNumber( fieldIndex, 0 ) );
        break;
      case ValueMetaInterface.TYPE_STRING:
        consumer.addBinary( Binary.fromString( row.getString( fieldIndex, null ) ) );
        break;
      case ValueMetaInterface.TYPE_BOOLEAN:
        consumer.addBoolean( row.getBoolean( fieldIndex, false ) );
        break;
      case ValueMetaInterface.TYPE_INTEGER:
        consumer.addLong( row.getInteger( fieldIndex, 0 ) );
        break;
      case ValueMetaInterface.TYPE_BIGNUMBER:
        consumer.addDouble( row.getNumber( fieldIndex, 0 ) );
        break;
      case ValueMetaInterface.TYPE_SERIALIZABLE:
        /**
         * 'fromByteArray' deprecated in the HDP, but CDH doesn't have 'fromReusedByteArray' yet.
         */
        //#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI"
        consumer.addBinary( Binary.fromReusedByteArray( row.getBinary( fieldIndex, new byte[ 0 ] ) ) );
        //#endif
        //$     consumer.addBinary( Binary.fromByteArray( row.getBinary( fieldIndex, new byte[0] ) ) );
        //#if shim_type=="CDH" || shim_type=="MAPR"
        //#endif
        break;
      case ValueMetaInterface.TYPE_BINARY:
        /**
         * 'fromByteArray' deprecated in the HDP, but CDH doesn't have 'fromReusedByteArray' yet.
         */
        //#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI"
        consumer.addBinary( Binary.fromReusedByteArray( row.getBinary( fieldIndex, new byte[ 0 ] ) ) );
        //#endif
        //$     consumer.addBinary( Binary.fromByteArray( row.getBinary( fieldIndex, new byte[0] ) ) );
        //#if shim_type=="CDH" || shim_type=="MAPR"
        //#endif
        break;
      case ValueMetaInterface.TYPE_DATE:
      case ValueMetaInterface.TYPE_TIMESTAMP:
        consumer.addLong( row.getDate( fieldIndex, new Date( 0 ) ).getTime() );
        break;
      default:
        throw new RuntimeException( "Undefined type: " + field.pentahoValueMetaType );
    }
    consumer.endField( field.formatFieldName, index );
  }

  public void writeRow( RowMetaAndData row, RecordConsumer consumer ) {
    consumer.startMessage();
    int index = 0;
    for ( SchemaDescription.Field f : schema ) {
      if ( f.formatFieldName == null ) {
        continue;
      }
      try {
        writeField( f, index, row, consumer );
        index++;
      } catch ( KettleValueException ex ) {
        throw new RuntimeException( ex );
      }
    }
    consumer.endMessage();
  }

  public static class MyRecordMaterializer extends RecordMaterializer<RowMetaAndData> {
    private MyGroupConverter root;

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
    private RowMeta fields = new RowMeta();
    protected RowMetaAndData current;
    private Converter[] converters;
    private int count;

    public MyGroupConverter( ParquetConverter converter ) {
      count = 0;
      for ( SchemaDescription.Field f : converter.schema ) {
        if ( f.formatFieldName != null ) {
          count++;
        }
      }
      converters = new Converter[count];
      int i = 0;
      for ( SchemaDescription.Field f : converter.schema ) {
        if ( f.formatFieldName == null ) {
          continue;
        }

        final int index = i;
        switch ( f.pentahoValueMetaType ) {
          case ValueMetaInterface.TYPE_NUMBER:
            fields.addValueMeta( new ValueMetaNumber( f.pentahoFieldName ) );
            converters[i] = new PrimitiveConverter() {
              @Override
              public void addDouble( double value ) {
                current.getData()[index] = value;
              }

              @Override
              public void addFloat( float value ) {
                current.getData()[index] = (double) value;
              }

              @Override
              public void addInt( int value ) {
                current.getData()[index] = (double) value;
              }

              @Override
              public void addLong( long value ) {
                current.getData()[index] = (double) value;
              }

              @Override
              public void addBoolean( boolean value ) {
                current.getData()[index] = value ? 1.0 : 0.0;
              }
            };
            break;
          case ValueMetaInterface.TYPE_INTEGER:
            fields.addValueMeta( new ValueMetaInteger( f.pentahoFieldName ) );
            converters[i] = new PrimitiveConverter() {
              @Override
              public void addDouble( double value ) {
                current.getData()[index] = Math.round( value );
              }

              @Override
              public void addFloat( float value ) {
                current.getData()[index] = (long) Math.round( value );
              }

              @Override
              public void addInt( int value ) {
                current.getData()[index] = (long) value;
              }

              @Override
              public void addLong( long value ) {
                current.getData()[index] = value;
              }

              @Override
              public void addBoolean( boolean value ) {
                current.getData()[index] = value ? 1L : 0L;
              }
            };
            break;
          case ValueMetaInterface.TYPE_BIGNUMBER:
            fields.addValueMeta( new ValueMetaBigNumber( f.pentahoFieldName ) );
            converters[i] = new PrimitiveConverter() {
              @Override
              public void addDouble( double value ) {
                current.getData()[index] = new BigDecimal( value );
              }

              @Override
              public void addFloat( float value ) {
                current.getData()[index] = new BigDecimal( value );
              }

              @Override
              public void addInt( int value ) {
                current.getData()[index] = new BigDecimal( value );
              }

              @Override
              public void addLong( long value ) {
                current.getData()[index] = new BigDecimal( value );
              }
            };
            break;
          case ValueMetaInterface.TYPE_STRING:
            fields.addValueMeta( new ValueMetaString( f.pentahoFieldName ) );
            converters[i] = new PrimitiveConverter() {
              @Override
              public void addBinary( Binary value ) {
                current.getData()[index] = value.toStringUsingUTF8();
              }

              @Override
              public void addBoolean( boolean value ) {
                current.getData()[index] = Boolean.toString( value );
              }

              @Override
              public void addDouble( double value ) {
                current.getData()[index] = Double.toString( value );
              }

              @Override
              public void addFloat( float value ) {
                current.getData()[index] = Float.toString( value );
              }

              @Override
              public void addInt( int value ) {
                current.getData()[index] = Integer.toString( value );
              }

              @Override
              public void addLong( long value ) {
                current.getData()[index] = Long.toString( value );
              }
            };
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            fields.addValueMeta( new ValueMetaBoolean( f.pentahoFieldName ) );
            converters[i] = new PrimitiveConverter() {
              public void addDouble( double value ) {
                current.getData()[index] = Math.round( value ) != 0;
              }

              @Override
              public void addFloat( float value ) {
                current.getData()[index] = Math.round( value ) != 0;
              }

              @Override
              public void addInt( int value ) {
                current.getData()[index] = value != 0;
              }

              @Override
              public void addBoolean( boolean value ) {
                current.getData()[index] = value;
              }
            };
            break;
          case ValueMetaInterface.TYPE_SERIALIZABLE:
            fields.addValueMeta( new ValueMetaSerializable( f.pentahoFieldName ) );
            converters[i] = new PrimitiveConverter() {
              @Override
              public void addBinary( Binary value ) {
                current.getData()[index] = value.getBytes();
              }
            };
            break;
          case ValueMetaInterface.TYPE_BINARY:
            fields.addValueMeta( new ValueMetaBinary( f.pentahoFieldName ) );
            converters[i] = new PrimitiveConverter() {
              @Override
              public void addBinary( Binary value ) {
                current.getData()[index] = value.getBytes();
              }
            };
            break;
          case ValueMetaInterface.TYPE_DATE:
            fields.addValueMeta( new ValueMetaDate( f.pentahoFieldName ) );
            converters[i] = new PrimitiveConverter() {
              @Override
              public void addLong( long value ) {
                current.getData()[index] = new Date( value );
              }

              // the number of days from the Unix epoch, 1 January 1970.
              @Override
              public void addInt( int value ) {
                current.getData()[index] = new Date( value * 24L * 60L * 60L * 1000L );
              }

              @Override
              public void addBinary( Binary value ) {
                current.getData()[index] = new Date( dateFromInt96( value ) );
              }
            };
            break;
          case ValueMetaInterface.TYPE_TIMESTAMP:
            fields.addValueMeta( new ValueMetaTimestamp( f.pentahoFieldName ) );
            converters[i] = new PrimitiveConverter() {
              @Override
              public void addLong( long value ) {
                current.getData()[index] = new Timestamp( value );
              }

              // the number of days from the Unix epoch, 1 January 1970.
              @Override
              public void addInt( int value ) {
                current.getData()[index] = new Timestamp( value * 24L * 60L * 60L * 1000L );
              }

              @Override
              public void addBinary( Binary value ) {
                current.getData()[index] = new Timestamp( dateFromInt96( value ) );
              }
            };
            break;
          default:
            throw new RuntimeException( "Undefined type: " + f.pentahoValueMetaType );
        }
        i++;
      }
    }

    private static final int JULIAN_DAY_OF_EPOCH = 2440588;

    private static long dateFromInt96( Binary value ) {
      byte[] readBuffer = value.getBytes();
      if ( readBuffer.length != 12 ) {
        throw new RuntimeException( "Invalid byte array length for INT96" );
      }

      long timeOfDayNanos =
          ( ( (long) readBuffer[7] << 56 ) + ( (long) ( readBuffer[6] & 255 ) << 48 )
              + ( (long) ( readBuffer[5] & 255 ) << 40 ) + ( (long) ( readBuffer[4] & 255 ) << 32 )
              + ( (long) ( readBuffer[3] & 255 ) << 24 ) + ( ( readBuffer[2] & 255 ) << 16 )
              + ( ( readBuffer[1] & 255 ) << 8 ) + ( ( readBuffer[0] & 255 ) << 0 ) );

      int julianDay =
          ( (int) ( readBuffer[11] & 255 ) << 24 ) + ( ( readBuffer[10] & 255 ) << 16 )
              + ( ( readBuffer[9] & 255 ) << 8 ) + ( ( readBuffer[8] & 255 ) << 0 );

      return ( julianDay - JULIAN_DAY_OF_EPOCH ) * 24L * 60L * 60L * 1000L + timeOfDayNanos / 1000000;
    }

    @Override
    public void start() {
      current = new RowMetaAndData( fields );
      current.setData( new Object[count] );
    }

    @Override
    public Converter getConverter( int fieldIndex ) {
      return converters[fieldIndex];
    }

    @Override
    public void end() {
    }

    public RowMetaAndData getCurrentRecord() {
      return current;
    }
  }
}
