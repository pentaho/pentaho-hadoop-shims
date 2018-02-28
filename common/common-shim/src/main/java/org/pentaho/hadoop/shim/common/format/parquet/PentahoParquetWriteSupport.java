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
package org.pentaho.hadoop.shim.common.format.parquet;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;

//#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI" || shim_name=="mapr60"
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
//#endif
//#if shim_type=="CDH" || shim_type=="MAPR" && shim_name!="mapr60"
//$import parquet.hadoop.api.WriteSupport;
//$import parquet.io.api.Binary;
//$import parquet.io.api.RecordConsumer;
//$import parquet.schema.MessageType;
//$import parquet.schema.OriginalType;
//$import parquet.schema.PrimitiveType;
//$import parquet.schema.Type;
//#endif

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.hadoop.shim.api.format.IParquetOutputField;

public class PentahoParquetWriteSupport extends WriteSupport<RowMetaAndData> {
  private RecordConsumer consumer;
  private List<? extends IParquetOutputField> outputFields;

  public PentahoParquetWriteSupport( List<? extends IParquetOutputField> outputFields ) {
    this.outputFields = outputFields;
  }

  @Override
  public WriteContext init( Configuration configuration ) {
    try {
      return new WriteContext( createParquetSchema(), new TreeMap<>() );
    } catch ( Exception ex ) {
      throw new RuntimeException( ex );
    }
  }

  @Override
  public void prepareForWrite( RecordConsumer recordConsumer ) {
    consumer = recordConsumer;
  }

  @Override
  public void write( RowMetaAndData record ) {
    writeRow( record, consumer );
  }

  private MessageType createParquetSchema() {
    List<Type> types = new ArrayList<>();

    for ( IParquetOutputField outputField : outputFields ) {
      types.add( convertToPrimitiveType( outputField ) );
    }

    if ( types.isEmpty() ) {
      throw new IllegalArgumentException( "Schema should contain at least one field" );
    }

    return new MessageType( "parquet-schema", types );
  }

  public void writeRow( RowMetaAndData row, RecordConsumer consumer ) {
    consumer.startMessage();
    int index = 0;
    for ( IParquetOutputField f : outputFields ) {
      if ( f.getFormatFieldName() == null ) {
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

  private void writeField( IParquetOutputField field, int index, RowMetaAndData row, RecordConsumer consumer )
    throws KettleValueException {
    RowMetaInterface rmi = row.getRowMeta();
    int fieldIndex = row.getRowMeta().indexOfValue( field.getPentahoFieldName() );
    ValueMetaInterface vmi = rmi.getValueMeta( fieldIndex );

    if ( fieldIndex < 0 ) {
      if ( field.getAllowNull() ) {
        return;
      } else {
        throw new KettleValueException( "Required field '" + field.getPentahoFieldName() + "' not found in rowset" );
      }
    }
    if ( row.isEmptyValue( field.getPentahoFieldName() ) ) {
      if ( field.getAllowNull() ) {
        return;
      } else {
        if ( field.getDefaultValue() == null ) {
          throw new KettleValueException(
            "Required field '" + field.getPentahoFieldName() + "' contains no data and default values not defined" );
        } else {
          // put default value
          consumer.startField( field.getFormatFieldName(), index );
          switch ( field.getParquetType() ) {
            case FLOAT:
              consumer.addFloat( Float.parseFloat( field.getDefaultValue() ) );
              break;
            case DOUBLE:
              consumer.addDouble( Double.parseDouble( field.getDefaultValue() ) );
              break;
            case BINARY:
            case UTF8:
              consumer.addBinary( Binary.fromString( field.getDefaultValue() ) );
              break;
            case BOOLEAN:
              consumer.addBoolean( Boolean.parseBoolean( field.getDefaultValue() ) );
              break;
            case INT_32:
              consumer.addInteger( Integer.parseInt( field.getDefaultValue() ) );
              break;
            case TIMESTAMP_MILLIS:
            case INT_64:
              consumer.addLong( Long.parseLong( field.getDefaultValue() ) );
              break;
            case DECIMAL:
//              consumer.addBinary( ? );
              break;
            case DATE:
              Date defaultDate = null;
              String defaultValue = field.getDefaultValue();
              String conversionMask = ( vmi.getConversionMask() == null ) ? ValueMetaBase.DEFAULT_DATE_PARSE_MASK : vmi.getConversionMask();
              DateFormat dateFormat = new SimpleDateFormat( conversionMask );
              try {
                defaultDate = dateFormat.parse( defaultValue );
              } catch ( ParseException pe ) {
                // Do nothing
              }
              LocalDate localDate = defaultDate.toInstant().atZone( ZoneId.systemDefault() ).toLocalDate();
              Integer dateInDays = Math.toIntExact( ChronoUnit.DAYS.between( LocalDate.ofEpochDay( 0 ), localDate ) );
              consumer.addInteger( dateInDays );
              break;
            default:
              throw new RuntimeException( "Undefined type: " + field.getPentahoType() );
          }
          consumer.endField( field.getFormatFieldName(), index );
          return;
        }
      }
    }
    consumer.startField( field.getFormatFieldName(), index );
    switch ( field.getParquetType() ) {
      case FLOAT:
        consumer.addFloat( (float) row.getNumber( fieldIndex, 0 ) );
        break;
      case DOUBLE:
        consumer.addDouble( row.getNumber( fieldIndex, 0 ) );
        break;
      case BINARY:
        byte[] bytes = row.getBinary( fieldIndex, null );
        //#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI"
        consumer.addBinary( Binary.fromConstantByteArray( bytes ) );
        //#endif
        //#if shim_type=="CDH" || shim_type=="MAPR"
        //$     consumer.addBinary( Binary.fromByteArray( bytes ) );
        //#endif
        break;
      case UTF8:
        consumer.addBinary( Binary.fromString( row.getString( fieldIndex, null ) ) );
        break;
      case BOOLEAN:
        consumer.addBoolean( row.getBoolean( fieldIndex, false ) );
        break;
      case INT_32:
        Long tmpLong = row.getInteger( fieldIndex, 0 );
        consumer.addInteger( tmpLong.intValue() );
        break;
      case TIMESTAMP_MILLIS:
        Date timeStamp = row.getDate( fieldIndex, null );
        consumer.addLong( timeStamp.getTime() );
        break;
      case INT_64:
        consumer.addLong( row.getInteger( fieldIndex, 0 ) );
        break;
      case DECIMAL:
//              consumer.addBinary( ? );
        break;
      case DATE:
        Date dateFromRow = row.getDate( fieldIndex, null );
        LocalDate localDate = dateFromRow.toInstant().atZone( ZoneId.systemDefault() ).toLocalDate();
        Integer dateInDays = Math.toIntExact( ChronoUnit.DAYS.between( LocalDate.ofEpochDay( 0 ), localDate ) );
        consumer.addInteger( dateInDays );
        break;
      default:
        throw new RuntimeException( "Undefined type: " + field.getPentahoType() );
    }

    consumer.endField( field.getFormatFieldName(), index );
  }





//  public static BigDecimal binaryToDecimal( Binary value, int precision, int scale) {
//    /*
//     * Precision <= 18 checks for the max number of digits for an unscaled long,
//     * else treat with big integer conversion
//     */
//    if (precision <= 18) {
//      ByteBuffer buffer = value.toByteBuffer();
//      byte[] bytes = buffer.array();
//      int start = buffer.arrayOffset() + buffer.position();
//      int end = buffer.arrayOffset() + buffer.limit();
//      long unscaled = 0L;
//      int i = start;
//      while ( i < end ) {
//        unscaled = ( unscaled << 8 | bytes[i] & 0xff );
//        i++;
//      }
//      int bits = 8*(end - start);
//      long unscaledNew = (unscaled << (64 - bits)) >> (64 - bits);
//      if (unscaledNew <= -pow(10,18) || unscaledNew >= pow(10,18)) {
//        return new BigDecimal(unscaledNew);
//      } else {
//        return BigDecimal.valueOf(unscaledNew / pow(10,scale));
//      }
//    } else {
//      return new BigDecimal(new BigInteger(value.getBytes()), scale);
//    }
//  }


  private PrimitiveType convertToPrimitiveType( IParquetOutputField f ) {
    Type.Repetition rep = f.getAllowNull() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;
    String formatFieldName = f.getFormatFieldName();
    switch ( f.getParquetType() ) {
      case BINARY:
        return new PrimitiveType( rep, PrimitiveType.PrimitiveTypeName.BINARY, formatFieldName );
      case BOOLEAN:
        return new PrimitiveType( rep, PrimitiveType.PrimitiveTypeName.BOOLEAN, formatFieldName );
      case DOUBLE:
        return new PrimitiveType( rep, PrimitiveType.PrimitiveTypeName.DOUBLE, formatFieldName );
      case FLOAT:
        return new PrimitiveType( rep, PrimitiveType.PrimitiveTypeName.FLOAT, formatFieldName );
      case INT_32:
        return new PrimitiveType( rep, PrimitiveType.PrimitiveTypeName.INT32, formatFieldName );
      case UTF8:
        return new PrimitiveType( rep, PrimitiveType.PrimitiveTypeName.BINARY, formatFieldName, OriginalType.UTF8 );
      case INT_64:
        return new PrimitiveType( rep, PrimitiveType.PrimitiveTypeName.INT64, formatFieldName, OriginalType.INT_64 );
      case DATE:
        return new PrimitiveType( rep, PrimitiveType.PrimitiveTypeName.INT32, formatFieldName, OriginalType.DATE );
      case DECIMAL:
        return new PrimitiveType( rep, PrimitiveType.PrimitiveTypeName.BINARY, formatFieldName, OriginalType.DECIMAL );
      case TIMESTAMP_MILLIS:
        return new PrimitiveType( rep, PrimitiveType.PrimitiveTypeName.INT64, formatFieldName, OriginalType.TIMESTAMP_MILLIS );
      default:
        throw new RuntimeException( "Unsupported output type: " + f.getParquetType() );
    }
  }
}
