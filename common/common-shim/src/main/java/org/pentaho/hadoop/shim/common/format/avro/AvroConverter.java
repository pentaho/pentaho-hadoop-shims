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
import org.apache.avro.util.Utf8;
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
  private static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
  private static final String DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

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
              Date timeStamp = row.getDate( fieldMetaIndex, defaultTimeStamp );
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
              Date dateFromRow = row.getDate( fieldMetaIndex, defaultDate );
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

  public static RowMetaAndData convertFromAvro( GenericRecord record, SchemaDescription avroSchemaDescription,
                                                SchemaDescription metaSchemaDescription  ) {
    return convertFromAvro( new RowMetaAndData(), record, avroSchemaDescription, metaSchemaDescription );
  }

  @VisibleForTesting static RowMetaAndData convertFromAvro( RowMetaAndData rowMetaAndData, GenericRecord avroRecord,
                                                            SchemaDescription avroSchemaDescription, SchemaDescription metaSchemaDescription ) {
    for ( SchemaDescription.Field metaField : metaSchemaDescription ) {
      SchemaDescription.Field avroField = avroSchemaDescription.getFormatField( metaField.formatFieldName );

      if ( avroField == null ) {
        return (RowMetaAndData) handleConversionError( "Field: " + metaField.formatFieldName + "  Does not exist in the avro file or schema" );
      }

      if ( metaField != null ) {
        String avroFieldVal = metaField.formatFieldName;
        // Does the metaField contains Pentaho metaField format NAME_DELIMITER_TYPE_DELIMETER_ALLOWNULL
        AvroSchemaConverter.FieldName
            avroFieldName =
            new AvroSchemaConverter.FieldName( avroField.formatFieldName, avroField.pentahoValueMetaType, avroField.allowNull );
        String avroName = avroFieldName.toString();
        Object avroRecordObject = avroRecord.get( avroName );
        if ( avroRecordObject != null ) {
          avroFieldVal = avroName;
        }

        Object data = convertFromSourceToTargetDataType( avroField.pentahoValueMetaType, metaField.pentahoValueMetaType, avroRecord.get( avroFieldVal ) );

        rowMetaAndData.addValue( metaField.pentahoFieldName, metaField.pentahoValueMetaType,  data );
      }
    }
    return rowMetaAndData;
  }

  protected static Object getAvroRecordFieldValue( GenericRecord avroRecord, String avroFieldVal ) {
    Object returnVal = null;

    returnVal = avroRecord.get( avroFieldVal );

    if ( returnVal instanceof  Utf8 ) {
      returnVal = returnVal.toString();
    }

    return returnVal;
  }

  protected static Object convertFromSourceToTargetDataType( int sourceValueMetaInterface, int targetValueMetaInterface, Object value ) {
    if ( value == null ) {
      return null;
    }

    if ( value instanceof  Utf8 ) {
      value = value.toString();
    }

    switch ( sourceValueMetaInterface ) {
      case ValueMetaInterface.TYPE_INET:
        return convertFromInetMetaInterface( targetValueMetaInterface, value );

      case ValueMetaInterface.TYPE_STRING:
        return convertFromStringMetaInterface( targetValueMetaInterface, value );

      case ValueMetaInterface.TYPE_INTEGER:
        return convertFromIntegerMetaInterface( targetValueMetaInterface, value );

      case ValueMetaInterface.TYPE_NUMBER:
        return convertFromNumberMetaInterface( targetValueMetaInterface, value );

      case ValueMetaInterface.TYPE_BIGNUMBER:
        return convertFromBigNumberMetaInterface( targetValueMetaInterface, value );

      case ValueMetaInterface.TYPE_TIMESTAMP:
        return convertFromTimestampMetaInterface( targetValueMetaInterface, value );

      case ValueMetaInterface.TYPE_DATE:
        return convertFromDateMetaInterface( targetValueMetaInterface, value );

      case ValueMetaInterface.TYPE_BOOLEAN:
        return convertFromBooleanMetaInterface( targetValueMetaInterface, value );

      case ValueMetaInterface.TYPE_BINARY:
        return convertFromBinaryMetaInterface( targetValueMetaInterface, value );
    }

    //if none of the cases match, return the original value passed in
    return value;
  }

  protected static Object convertFromStringMetaInterface( int targetValueMetaInterface, Object value ) {
    Object returnVal = value;
    SimpleDateFormat datePattern = new SimpleDateFormat( DEFAULT_DATE_FORMAT );
    SimpleDateFormat timestampPattern = new SimpleDateFormat( DEFAULT_TIMESTAMP_FORMAT );

    if ( value == null ) {
      return value;
    }

    // value is expected to be of type String
    if ( !( value instanceof String ) ) {
      return handleConversionError( "Error.  Expecting value of type string.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );
    }

    try {
      switch ( targetValueMetaInterface ) {
        case ValueMetaInterface.TYPE_INET:
          if ( value != null ) {
            try {
              returnVal = InetAddress.getByName( value.toString() );
            } catch ( UnknownHostException e ) {
              returnVal = null;
            }
          }
          break;
        case ValueMetaInterface.TYPE_STRING:
          // if we're going from string to string then just return the value - no conversion needed
          returnVal = value;
          break;
        case ValueMetaInterface.TYPE_INTEGER:
          returnVal = Long.parseLong( (String) value );
          break;
        case ValueMetaInterface.TYPE_NUMBER:
          returnVal = Double.parseDouble( (String) value );
          break;
        case ValueMetaInterface.TYPE_BIGNUMBER:
          Double doubleValue = Double.parseDouble( (String) value );

          returnVal = new BigDecimal( doubleValue );
          break;
        case ValueMetaInterface.TYPE_TIMESTAMP:
          returnVal = new Timestamp( ( timestampPattern.parse( (String) value ) ).getTime() );

          break;
        case ValueMetaInterface.TYPE_DATE:
          returnVal = datePattern.parse( (String) value );

          break;
        case ValueMetaInterface.TYPE_BOOLEAN:
          returnVal = Boolean.parseBoolean( (String) value );
          break;
        case ValueMetaInterface.TYPE_BINARY:
          returnVal = ( (String) value ).getBytes();
          break;
      }
    } catch ( Exception e ) {
      return handleConversionError( "Error trying to convert from String to a different data type.  value = '" + value + "'.  Error:  " + e.getClass() + ":  " + e.getMessage(), e );
    }

    return returnVal;
  }

  protected static Object convertFromDateMetaInterface( int targetValueMetaInterface, Object value ) {
    Object returnVal = value;

    if ( value == null ) {
      return value;
    }

    // value is expected to be of type Integer
    if ( !( value instanceof Integer ) ) {
      return handleConversionError( "Error.  Expecting value of type Integer.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );
    }

    try {
      LocalDate localDate = LocalDate.ofEpochDay( 0 ).plusDays( (Integer) value );

      Date dateValue = Date.from( localDate.atStartOfDay( ZoneId.systemDefault() ).toInstant() );

      switch ( targetValueMetaInterface ) {
        case ValueMetaInterface.TYPE_INET:
          return handleConversionError( "Error.  Can not convert from Date to Inet.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_STRING:
          SimpleDateFormat datePattern = new SimpleDateFormat( DEFAULT_DATE_FORMAT );

          returnVal = datePattern.format( dateValue );

          break;

        case ValueMetaInterface.TYPE_INTEGER:
          return handleConversionError( "Error.  Can not convert from Date to Integer.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_NUMBER:
          return handleConversionError( "Error.  Can not convert from Date to Number.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_BIGNUMBER:
          return handleConversionError( "Error.  Can not convert from Date to BigNumber.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_TIMESTAMP:
          returnVal = new Timestamp( dateValue.getTime() );
          break;

        case ValueMetaInterface.TYPE_DATE:
          returnVal = dateValue;
          break;

        case ValueMetaInterface.TYPE_BOOLEAN:
          return handleConversionError( "Error.  Can not convert from Date to Boolean.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_BINARY:
          return handleConversionError( "Error.  Can not convert from Date to Binary.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );
      }
    } catch ( Exception e ) {
      return handleConversionError( "Error trying to convert from Date to a different data type.  value = '" + value + "'.  Error:  " + e.getClass() + ":  " + e.getMessage(), e );
    }

    return returnVal;
  }

  protected static Object convertFromNumberMetaInterface( int targetValueMetaInterface, Object value ) {
    Object returnVal = value;

    if ( value == null ) {
      return value;
    }

    // value is expected to be of type Double
    if ( !( value instanceof Double ) ) {
      return handleConversionError( "Error.  Expecting value of type Double.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );
    }

    try {
      switch ( targetValueMetaInterface ) {
        case ValueMetaInterface.TYPE_INET:
          return handleConversionError( "Error.  Can not convert from Number to Inet.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_STRING:
          break;

        case ValueMetaInterface.TYPE_INTEGER:
          return handleConversionError( "Error.  Can not convert from Number to Integer.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_NUMBER:
          returnVal = value;
          break;

        case ValueMetaInterface.TYPE_BIGNUMBER:
          returnVal = new BigDecimal( (Double) value );
          break;

        case ValueMetaInterface.TYPE_TIMESTAMP:
          return handleConversionError( "Error.  Can not convert from Number to Timestamp.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_DATE:
          return handleConversionError( "Error.  Can not convert from Number to Date.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_BOOLEAN:
          return handleConversionError( "Error.  Can not convert from Number to Boolean.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_BINARY:
          return handleConversionError( "Error.  Can not convert from Number to Binary.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );
      }
    } catch ( Exception e ) {
      return handleConversionError( "Error trying to convert from Number to a different data type.  value = '" + value + "'.  Error:  " + e.getClass() + ":  " + e.getMessage(), e );
    }

    return returnVal;
  }

  protected static Object convertFromBooleanMetaInterface( int targetValueMetaInterface, Object value ) {
    Object returnVal = value;

    if ( value == null ) {
      return value;
    }

    // value is expected to be of type String
    if ( !( value instanceof Boolean ) ) {
      return handleConversionError( "Error.  Expecting value of type string.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );
    }

    try {
      switch ( targetValueMetaInterface ) {
        case ValueMetaInterface.TYPE_INET:
          return handleConversionError( "Error.  Can not convert from Boolean to Inet.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_STRING:
          returnVal = Boolean.toString( (Boolean) value );
          break;

        case ValueMetaInterface.TYPE_INTEGER:
          return handleConversionError( "Error.  Can not convert from Boolean to Integer.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_NUMBER:
          return handleConversionError( "Error.  Can not convert from Boolean to Number.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_BIGNUMBER:
          return handleConversionError( "Error.  Can not convert from Boolean to BigNumber.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_TIMESTAMP:
          return handleConversionError( "Error.  Can not convert from Boolean to Timestamp.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_DATE:
          return handleConversionError( "Error.  Can not convert from Boolean to Date.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_BOOLEAN:
          returnVal = value;
          break;

        case ValueMetaInterface.TYPE_BINARY:
          return handleConversionError( "Error.  Can not convert from Boolean to Binary.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );
      }
    } catch ( Exception e ) {
      return handleConversionError( "Error trying to convert from String to a different data type.  value = '" + value + "'.  Error:  " + e.getClass() + ":  " + e.getMessage(), e );
    }

    return returnVal;
  }

  protected static Object convertFromIntegerMetaInterface( int targetValueMetaInterface, Object value ) {
    Object returnVal = value;

    if ( value == null ) {
      return value;
    }

    // value is expected to be of type Long
    if ( !( value instanceof Long ) ) {
      return handleConversionError( "Error.  Expecting value of type Long.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );
    }

    try {
      switch ( targetValueMetaInterface ) {
        case ValueMetaInterface.TYPE_INET:
          return handleConversionError( "Error.  Can not convert from Integer to Inet.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_STRING:
          returnVal = Long.toString( (Long) value );
          break;

        case ValueMetaInterface.TYPE_INTEGER:
          returnVal = (Long) value;
          break;

        case ValueMetaInterface.TYPE_NUMBER:
          returnVal = ( (Long) value ).doubleValue();
          break;

        case ValueMetaInterface.TYPE_BIGNUMBER:
          returnVal = new BigDecimal( ( (Long) value ).doubleValue() );
          break;

        case ValueMetaInterface.TYPE_TIMESTAMP:
          return handleConversionError( "Error.  Can not convert from Integer to Timestamp.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_DATE:
          return handleConversionError( "Error.  Can not convert from Integer to Date.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_BOOLEAN:
          return handleConversionError( "Error.  Can not convert from Integer to Boolean.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_BINARY:
          return handleConversionError( "Error.  Can not convert from Integer to Binary.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );
      }
    } catch ( Exception e ) {
      return handleConversionError( "Error trying to convert from Integer to a different data type.  value = '" + value + "'.  Error:  " + e.getClass() + ":  " + e.getMessage(), e );
    }

    return returnVal;
  }

  protected static Object convertFromBigNumberMetaInterface( int targetValueMetaInterface, Object value ) {
    Object returnVal = value;

    if ( value == null ) {
      return value;
    }

    // value is expected to be of type Double
    if ( !( value instanceof Double ) ) {
      return handleConversionError( "Error.  Expecting value of type Double.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );
    }

    try {
      switch ( targetValueMetaInterface ) {
        case ValueMetaInterface.TYPE_INET:
          return handleConversionError( "Error.  Can not convert from BigNumber to Inet.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_STRING:
          returnVal = (  (Double) value ).toString();
          break;

        case ValueMetaInterface.TYPE_INTEGER:
          return handleConversionError( "Error.  Can not convert from BigNumber to Integer.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_NUMBER:
          returnVal = (  (Double) value );
          break;

        case ValueMetaInterface.TYPE_BIGNUMBER:
          returnVal = new BigDecimal( ( (Double) value ) );
          break;

        case ValueMetaInterface.TYPE_TIMESTAMP:
          return handleConversionError( "Error.  Can not convert from BigNumber to Timestamp.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_DATE:
          return handleConversionError( "Error.  Can not convert from BigNumber to Date.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_BOOLEAN:
          return handleConversionError( "Error.  Can not convert from BigNumber to Boolean.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_BINARY:
          return handleConversionError( "Error.  Can not convert from BigNumber to Binary.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );
      }
    } catch ( Exception e ) {
      return handleConversionError( "Error trying to convert from BigNumber to a different data type.  value = '" + value + "'.  Error:  " + e.getClass() + ":  " + e.getMessage(), e );
    }

    return returnVal;
  }

  protected static Object convertFromTimestampMetaInterface( int targetValueMetaInterface, Object value ) {
    Object returnVal = value;

    if ( value == null ) {
      return value;
    }

    // value is expected to be of type String
    if ( !( value instanceof Long ) ) {
      return handleConversionError( "Error.  Expecting value of type string.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );
    }

    try {
      switch ( targetValueMetaInterface ) {
        case ValueMetaInterface.TYPE_INET:
          return handleConversionError( "Error.  Can not convert from Timnestamp to Inet.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_STRING:
          Date dateValue = new Date( (Long) value );

          SimpleDateFormat timestampPattern = new SimpleDateFormat( DEFAULT_TIMESTAMP_FORMAT );

          returnVal = timestampPattern.format( dateValue );

          break;

        case ValueMetaInterface.TYPE_INTEGER:
          return handleConversionError( "Error.  Can not convert from Timnestamp to Integer.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_NUMBER:
          return handleConversionError( "Error.  Can not convert from Timnestamp to Number.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_BIGNUMBER:
          return handleConversionError( "Error.  Can not convert from Timnestamp to BigNumber.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_TIMESTAMP:
          returnVal = new Timestamp( (Long) value );
          break;

        case ValueMetaInterface.TYPE_DATE:
          returnVal = new Date( (Long) value );
          break;

        case ValueMetaInterface.TYPE_BOOLEAN:
          return handleConversionError( "Error.  Can not convert from Timnestamp to Boolean.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_BINARY:
          return handleConversionError( "Error.  Can not convert from Timnestamp to Binary.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );
      }
    } catch ( Exception e ) {
      return handleConversionError( "Error trying to convert from String to a different data type.  value = '" + value + "'.  Error:  " + e.getClass() + ":  " + e.getMessage(), e );
    }

    return returnVal;
  }

  protected static Object convertFromInetMetaInterface( int targetValueMetaInterface, Object value ) {
    Object returnVal = value;

    if ( value == null ) {
      return value;
    }

    // value is expected to be of type String
    if ( !( value instanceof String ) ) {
      return handleConversionError( "Error.  Expecting value of type string.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );
    }

    try {
      switch ( targetValueMetaInterface ) {
        case ValueMetaInterface.TYPE_INET:
          try {
            returnVal = InetAddress.getByName( value.toString() );
          } catch ( UnknownHostException e ) {
            returnVal = null;
          }
          break;

        case ValueMetaInterface.TYPE_STRING:
          try {
            returnVal = InetAddress.getByName( (String) value ).toString();
          } catch ( UnknownHostException e ) {
            returnVal = null;
          }
          break;

        case ValueMetaInterface.TYPE_INTEGER:
          return handleConversionError( "Error.  Can not convert from Inet to Integer.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_NUMBER:
          return handleConversionError( "Error.  Can not convert from Inet to Number.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_BIGNUMBER:
          return handleConversionError( "Error.  Can not convert from Inet to BigNumber.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_TIMESTAMP:
          return handleConversionError( "Error.  Can not convert from Inet to Timestamp.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_DATE:
          return handleConversionError( "Error.  Can not convert from Inet to Date.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_BOOLEAN:
          return handleConversionError( "Error.  Can not convert from Inet to Boolean.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_BINARY:
          return handleConversionError( "Error.  Can not convert from Inet to Binary.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );
      }
    } catch ( Exception e ) {
      return handleConversionError( "Error trying to convert from String to a different data type.  value = '" + value + "'.  Error:  " + e.getClass() + ":  " + e.getMessage(), e );
    }

    return returnVal;
  }

  protected static Object convertFromBinaryMetaInterface( int targetValueMetaInterface, Object value ) {
    Object returnVal = value;

    if ( value == null ) {
      return value;
    }

    // value is expected to be of type ByteBuffer
    if ( !( value instanceof ByteBuffer ) ) {
      return handleConversionError( "Error.  Expecting value of type ByteBuffer.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );
    }

    try {
      switch ( targetValueMetaInterface ) {
        case ValueMetaInterface.TYPE_INET:
          return handleConversionError( "Error.  Can not convert from Binary to Inet.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_STRING:
          return handleConversionError( "Error.  Can not convert from Binary to String.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_INTEGER:
          return handleConversionError( "Error.  Can not convert from Binary to Integer.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_NUMBER:
          return handleConversionError( "Error.  Can not convert from Binary to Number.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_BIGNUMBER:
          return handleConversionError( "Error.  Can not convert from Binary to BigNumber.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_TIMESTAMP:
          return handleConversionError( "Error.  Can not convert from Binary to Timestamp.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_DATE:
          return handleConversionError( "Error.  Can not convert from Binary to Date.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_BOOLEAN:
          return handleConversionError( "Error.  Can not convert from Binary to Boolean.    actual value type = '" + value.getClass() + "'.    value = '" + value + "'." );

        case ValueMetaInterface.TYPE_BINARY:
          returnVal = ( (ByteBuffer) value ).array();
          break;
      }
    } catch ( Exception e ) {
      return handleConversionError( "Error trying to convert from String to a different data type.  value = '" + value + "'.  Error:  " + e.getClass() + ":  " + e.getMessage(), e );
    }

    return returnVal;
  }

  protected static Object handleConversionError( String errorMessage ) {
    return handleConversionError( errorMessage, null );
  }

  protected static Object handleConversionError( String errorMessage, Exception e ) {
//      TODO - log an error message to let the user know there's a problem.  For now, return null
    return null;
  }
}

