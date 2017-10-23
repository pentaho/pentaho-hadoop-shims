/*! ******************************************************************************
 *
 * Pentaho Big Data
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


import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;

/**
 * Created by rmansoor on 10/7/2017.
 */
public class AvroConverter {

  public static GenericRecord convertToAvro( RowMetaAndData row, Schema schema, SchemaDescription schemaDescription ) {
    RowMetaInterface rmi = row.getRowMeta();
    GenericRecord outputRecord = new GenericData.Record( schema );

    try {
      //Build the avro row
      for ( SchemaDescription.Field field : schemaDescription ) {
        if ( field != null ) {
          String fieldVal = field.formatFieldName;
          // Does the field contains Pentaho field format NAME_DELIMITER_TYPE_DELIMETER_ALLOWNULL
          AvroSchemaConverter.FieldName
              fieldName =
              new AvroSchemaConverter.FieldName( field.formatFieldName, field.pentahoValueMetaType, field.allowNull );
          String name = fieldName.toString();
          Schema.Field schemaField = schema.getField( name );

          if ( schemaField != null ) {
            fieldVal = schemaField.name();
          }
          int fieldMetaIndex = rmi.indexOfValue( field.pentahoFieldName );
          ValueMetaInterface vmi = rmi.getValueMeta( fieldMetaIndex );
          switch ( vmi.getType() ) {
            case ValueMetaInterface.TYPE_INET:
            case ValueMetaInterface.TYPE_STRING:
              outputRecord.put( fieldVal, row.getString( fieldMetaIndex, String.valueOf( field.defaultValue ) ) );
              break;
            case ValueMetaInterface.TYPE_INTEGER:
              if ( field.defaultValue != null && field.defaultValue.length() > 0 ) {
                outputRecord.put( fieldVal, row.getInteger( fieldMetaIndex, Long.parseLong( field.defaultValue ) ) );
              } else {
                outputRecord.put( fieldVal, row.getInteger( fieldMetaIndex ) );
              }
              break;
            case ValueMetaInterface.TYPE_NUMBER:
              outputRecord.put( fieldVal, row.getNumber( fieldMetaIndex,
                  ( field.defaultValue != null && field.defaultValue.length() > 0 ) ? Double.parseDouble( field.defaultValue ) : 0 ) );
              break;
            case ValueMetaInterface.TYPE_BIGNUMBER:
              if ( field.defaultValue != null && field.defaultValue.length() > 0 ) {
                BigDecimal defaultBigDecimal = new BigDecimal( field.defaultValue );
                BigDecimal bigDecimal = row.getBigNumber( fieldMetaIndex, defaultBigDecimal );
                outputRecord.put( fieldVal, bigDecimal.doubleValue() );
              } else {
                BigDecimal bigDecimal = row.getBigNumber( fieldMetaIndex, null );
                if ( bigDecimal != null ) {
                  outputRecord.put( fieldVal, bigDecimal.doubleValue() );
                } else {
                  outputRecord.put( fieldVal, null );
                }
              }
              break;
            case ValueMetaInterface.TYPE_TIMESTAMP:
              Date defaultTimeStamp = null;
              if ( field.defaultValue != null && field.defaultValue.length() > 0 ) {
                DateFormat dateFormat = new SimpleDateFormat( vmi.getConversionMask() );
                try {
                  defaultTimeStamp = dateFormat.parse( field.defaultValue );
                } catch ( ParseException pe ) {
                  defaultTimeStamp = null;
                }
              }
              Date timeStamp =  row.getDate( fieldMetaIndex, defaultTimeStamp );
              outputRecord.put( fieldVal, timeStamp.getTime() );
              break;
            case ValueMetaInterface.TYPE_DATE:
              Date defaultDate = null;
              if ( field.defaultValue != null && field.defaultValue.length() > 0 ) {
                DateFormat dateFormat = new SimpleDateFormat( vmi.getConversionMask() );
                try {
                  defaultDate = dateFormat.parse( field.defaultValue );
                } catch ( ParseException pe ) {
                  defaultDate = null;
                }
              }
              Date dateFromRow =  row.getDate( fieldMetaIndex, defaultDate );
              LocalDate rowDate = dateFromRow.toInstant().atZone( ZoneId.systemDefault() ).toLocalDate();
              outputRecord.put( fieldVal, Math.toIntExact( ChronoUnit.DAYS.between( LocalDate.ofEpochDay( 0 ), rowDate ) ) );
              break;
            case ValueMetaInterface.TYPE_BOOLEAN:
              outputRecord.put( fieldVal, row.getBoolean( fieldMetaIndex, Boolean.parseBoolean( field.defaultValue ) ) );
              break;
            case ValueMetaInterface.TYPE_BINARY:
              if ( field.defaultValue != null && field.defaultValue.length() > 0 ) {
                outputRecord.put( fieldVal, ByteBuffer.wrap( row.getBinary( fieldMetaIndex, vmi.getBinary( field.defaultValue.getBytes() ) ) ) );
              } else {
                outputRecord.put( fieldVal, ByteBuffer.wrap( row.getBinary( fieldMetaIndex, new byte[0] ) ) );
              }
              break;
            default:
              break;
          }
        }
      }
    } catch ( ArithmeticException e ) {
      throw new IllegalArgumentException( "The date has too much day from epoch day!", e );
    } catch ( KettleValueException e ) {
      throw new IllegalArgumentException( "some exception while writing avro", e );
    }
    return outputRecord;
  }

  public static RowMetaAndData convertFromAvro( GenericRecord record, SchemaDescription schemaDescription ) {
    return convertFromAvro( new RowMetaAndData(), record, schemaDescription );
  }

  @VisibleForTesting static RowMetaAndData convertFromAvro( RowMetaAndData rowMetaAndData, GenericRecord record, SchemaDescription schemaDescription ) {
    for ( SchemaDescription.Field field : schemaDescription ) {
      if ( field != null ) {
        String fieldVal = field.formatFieldName;
        // Does the field contains Pentaho field format NAME_DELIMITER_TYPE_DELIMETER_ALLOWNULL
        AvroSchemaConverter.FieldName
            fieldName =
            new AvroSchemaConverter.FieldName( field.formatFieldName, field.pentahoValueMetaType, field.allowNull );
        String name = fieldName.toString();
        Object recordObject = record.get( name );
        if ( recordObject != null ) {
          fieldVal = name;
        }

        switch ( field.pentahoValueMetaType ) {
          case ValueMetaInterface.TYPE_INET:
            InetAddress address = null;
            Object data = record.get( fieldVal );
            if ( data != null ) {
              try {
                address = InetAddress.getByName( data.toString() );
              } catch ( UnknownHostException e ) {
                address = null;
              }
            }
            rowMetaAndData.addValue( field.pentahoFieldName, ValueMetaInterface.TYPE_INET,  address );
            break;
          case ValueMetaInterface.TYPE_STRING:
            rowMetaAndData.addValue( field.pentahoFieldName, ValueMetaInterface.TYPE_STRING,
                record.get( fieldVal ) );
            break;
          case ValueMetaInterface.TYPE_INTEGER:
            rowMetaAndData.addValue( field.pentahoFieldName, ValueMetaInterface.TYPE_INTEGER,
                record.get( fieldVal ) );
            break;
          case ValueMetaInterface.TYPE_NUMBER:
            rowMetaAndData.addValue( field.pentahoFieldName, ValueMetaInterface.TYPE_NUMBER, record.get( fieldVal ) );
            break;
          case ValueMetaInterface.TYPE_BIGNUMBER:
            BigDecimal bigDecimal = null;
            Double doubleValue;
            Object value = record.get( fieldVal );
            if ( value != null ) {
              if ( value instanceof Double ) {
                doubleValue = (Double) value;
              } else if ( value instanceof String ) {
                doubleValue = Double.parseDouble( (String) value );
              } else {
                throw new RuntimeException( "Unable to parse the value of Field: " + fieldVal );
              }
              bigDecimal = new BigDecimal( doubleValue );
            }
            rowMetaAndData.addValue( field.pentahoFieldName, ValueMetaInterface.TYPE_BIGNUMBER, bigDecimal );
            break;
          case ValueMetaInterface.TYPE_TIMESTAMP:
            Timestamp timestamp = null;
            Long longTimeStamp = (Long) record.get( fieldVal );
            if ( longTimeStamp != null ) {
              timestamp = new Timestamp( longTimeStamp );
            }
            rowMetaAndData.addValue( field.pentahoFieldName, ValueMetaInterface.TYPE_TIMESTAMP,  timestamp );
            break;
          case ValueMetaInterface.TYPE_DATE:
            Date date = null;
            Integer dateAsInteger = (Integer) record.get( fieldVal );
            if ( dateAsInteger != null ) {
              LocalDate localDate = LocalDate.ofEpochDay( 0 ).plusDays( dateAsInteger );
              date = Date.from( localDate.atStartOfDay( ZoneId.systemDefault() ).toInstant() );
            }
            rowMetaAndData.addValue( field.pentahoFieldName, ValueMetaInterface.TYPE_DATE, date );
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            rowMetaAndData.addValue( field.pentahoFieldName, ValueMetaInterface.TYPE_BOOLEAN, record.get( fieldVal ) );
            break;
          case ValueMetaInterface.TYPE_BINARY:
            ByteBuffer byteBuffer = (ByteBuffer) record.get( fieldVal );
            byte[] byteArray = new byte[0];
            if ( byteBuffer != null ) {
              byteArray = new byte[byteBuffer.remaining()];
              byteBuffer.get( byteArray );
            }
            rowMetaAndData.addValue( field.pentahoFieldName, ValueMetaInterface.TYPE_BINARY, byteArray );
            break;
          default:
            throw new RuntimeException( "Field: " + field.formatFieldName + "  Undefined type: " + field.pentahoValueMetaType );
        }
      }
    }
    return rowMetaAndData;
  }
}
