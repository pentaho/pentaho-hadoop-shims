/*******************************************************************************
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

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.hadoop.shim.api.format.AvroSpec;
import org.pentaho.hadoop.shim.api.format.IAvroInputField;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;

public class AvroToPdiConverter {
  Schema avroSchema;
  AvroSpec.DataType avroDataType = null;

  public AvroToPdiConverter( Schema avroSchema ) {
    this.avroSchema = avroSchema;
  }

  public Object converAvroToPdi( Object avroData, AvroInputField avroInputField, Schema avroField ) {

    //Might need to make getting the avroFieldName smarter here
    String avroFieldName = avroInputField.getPathParts().get( avroInputField.getPathParts().size() - 1 );
    Schema.Type primitiveAvroType = avroField.getType();

    primitiveAvroType = avroField.getType();

    switch ( primitiveAvroType ) {
      case INT:
        avroDataType = AvroSpec.DataType.INTEGER;
        break;
      case LONG:
        avroDataType = AvroSpec.DataType.LONG;
        break;
      case BYTES:
        avroDataType = AvroSpec.DataType.BYTES;
        break;
      case FIXED:
        avroDataType = AvroSpec.DataType.FIXED;
        break;
      case FLOAT:
        avroDataType = AvroSpec.DataType.FLOAT;
        break;
      case DOUBLE:
        avroDataType = AvroSpec.DataType.DOUBLE;
        break;
      case STRING:
        avroDataType = AvroSpec.DataType.STRING;
        break;
      case BOOLEAN:
        avroDataType = AvroSpec.DataType.BOOLEAN;
        break;
    }

    Object pentahoData = null;

    if ( avroData != null ) {
      if ( avroData instanceof Utf8 ) {
        avroData = avroData.toString();
      }

      int pentahoType = avroInputField.getPentahoType();
      switch ( avroDataType ) {
        case BOOLEAN:
          pentahoData = convertToPentahoType( pentahoType, (Boolean) avroData );
          break;
        case DATE:
          pentahoData = convertToPentahoType( pentahoType, (Integer) avroData );
          break;
        case FLOAT:
          pentahoData = convertToPentahoType( pentahoType, (Float) avroData );
          break;
        case DOUBLE:
          pentahoData = convertToPentahoType( pentahoType, (Double) avroData );
          break;
        case LONG:
          pentahoData = convertToPentahoType( pentahoType, (Long) avroData );
          break;
        case DECIMAL:
          pentahoData = convertToPentahoType( pentahoType, (ByteBuffer) avroData, avroField );
          break;
        case INTEGER:
          pentahoData = convertToPentahoType( pentahoType, (Integer) avroData );
          break;
        case STRING:
          if ( avroData instanceof GenericData.EnumSymbol ) {
            pentahoData = ( (GenericData.EnumSymbol) avroData ).toString();
          } else {
            pentahoData = convertToPentahoType( pentahoType, (String) avroData, avroInputField );
          }
          break;
        case BYTES:
          pentahoData = convertToPentahoType( pentahoType, (ByteBuffer) avroData, avroField );
          break;
        case FIXED:
          pentahoData = convertToPentahoType( pentahoType, ( (GenericData.Fixed) avroData ).bytes(), avroField );
          break;
        case TIMESTAMP_MILLIS:
          pentahoData = convertToPentahoType( pentahoType, (Long) avroData );
          break;
      }
    }

    return pentahoData;
  }

  private Object convertToPentahoType( int pentahoType, Float avroData ) {
    Object pentahoData = null;
    if ( avroData != null ) {
      try {
        switch ( pentahoType ) {
          case ValueMetaInterface.TYPE_STRING:
            pentahoData = avroData.toString();
            break;
          case ValueMetaInterface.TYPE_INTEGER:
            pentahoData = new Long( avroData.longValue() );
            break;
          case ValueMetaInterface.TYPE_NUMBER:
            pentahoData = Double.parseDouble( avroData.toString() );
            break;
          case ValueMetaInterface.TYPE_BIGNUMBER:
            new BigDecimal( avroData.toString() );
            break;
          case ValueMetaInterface.TYPE_TIMESTAMP:
            pentahoData = new Timestamp( avroData.longValue() );
            break;
          case ValueMetaInterface.TYPE_DATE:
            pentahoData = new Date( avroData.longValue() );
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            pentahoData = ( avroData == 0 ? Boolean.FALSE : Boolean.TRUE );
            break;
        }
      } catch ( Exception e ) {
        // If unable to do the type conversion just ignore. null will be returned.
      }
    }
    return pentahoData;
  }

  private Object convertToPentahoType( int pentahoType, ByteBuffer avroData, Schema field ) {
    Object pentahoData = null;
    if ( avroData != null ) {
      try {
        switch ( pentahoType ) {
          case ValueMetaInterface.TYPE_BIGNUMBER:
            Conversions.DecimalConversion converter = new Conversions.DecimalConversion();
            Schema schema = field;
            if ( schema.getType().equals( Schema.Type.UNION ) ) {
              List<Schema> schemas = field.getTypes();
              for ( Schema s : schemas ) {
                if ( !s.getName().equalsIgnoreCase( "null" ) ) {
                  schema = s;
                  break;
                }
              }
            }
            Object precision = schema.getObjectProp( AvroSpec.DECIMAL_PRECISION );
            Object scale = schema.getObjectProp( AvroSpec.DECIMAL_SCALE );
            LogicalTypes.Decimal decimalType =
              LogicalTypes.decimal( Integer.parseInt( precision.toString() ), Integer.parseInt( scale.toString() ) );
            pentahoData = converter.fromBytes( avroData, avroSchema, decimalType );
            break;
          case ValueMetaInterface.TYPE_BINARY:
            pentahoData = new byte[ avroData.remaining() ];
            avroData.get( (byte[]) pentahoData );
            break;
        }
      } catch ( Exception e ) {
        // If unable to do the type conversion just ignore. null will be returned.
      }
    }
    return pentahoData;
  }

  private Object convertToPentahoType( int pentahoType, Long avroData ) {
    Object pentahoData = null;
    if ( avroData != null ) {
      try {
        switch ( pentahoType ) {
          case ValueMetaInterface.TYPE_STRING:
            pentahoData = avroData.toString();
            break;
          case ValueMetaInterface.TYPE_INTEGER:
            pentahoData = avroData.longValue();
            break;
          case ValueMetaInterface.TYPE_NUMBER:
            pentahoData = new Double( avroData.doubleValue() );
            break;
          case ValueMetaInterface.TYPE_BIGNUMBER:
            pentahoData = BigDecimal.valueOf( avroData );
            break;
          case ValueMetaInterface.TYPE_TIMESTAMP:
            pentahoData = new Timestamp( avroData );
            break;
          case ValueMetaInterface.TYPE_DATE:
            pentahoData = new Date( avroData );
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            pentahoData = ( avroData == 0 ? Boolean.FALSE : Boolean.TRUE );
            break;
        }
      } catch ( Exception e ) {
        // If unable to do the type conversion just ignore. null will be returned.
      }
    }
    return pentahoData;
  }

  private Object convertToPentahoType( int pentahoType, Integer avroData ) {
    Object pentahoData = null;
    if ( avroData != null ) {
      try {
        switch ( pentahoType ) {
          case ValueMetaInterface.TYPE_STRING:
            pentahoData = avroData.toString();
            break;
          case ValueMetaInterface.TYPE_INTEGER:
            pentahoData = new Long( avroData.longValue() );
            break;
          case ValueMetaInterface.TYPE_NUMBER:
            pentahoData = new Double( avroData.doubleValue() );
            break;
          case ValueMetaInterface.TYPE_BIGNUMBER:
            pentahoData = BigDecimal.valueOf( avroData );
            break;
          case ValueMetaInterface.TYPE_TIMESTAMP:
            pentahoData = new Timestamp( avroData );
            break;
          case ValueMetaInterface.TYPE_DATE:
            LocalDate localDate = LocalDate.ofEpochDay( 0 ).plusDays( avroData );
            pentahoData = Date.from( localDate.atStartOfDay( ZoneId.systemDefault() ).toInstant() );
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            pentahoData = ( avroData == 0 ? Boolean.FALSE : Boolean.TRUE );
            break;
        }
      } catch ( Exception e ) {
        // If unable to do the type conversion just ignore. null will be returned.
      }
    }
    return pentahoData;
  }

  private Object convertToPentahoType( int pentahoType, Double avroData ) {
    Object pentahoData = null;
    if ( avroData != null ) {
      try {
        switch ( pentahoType ) {
          case ValueMetaInterface.TYPE_STRING:
            pentahoData = avroData.toString();
            break;
          case ValueMetaInterface.TYPE_INTEGER:
            pentahoData = new Long( avroData.longValue() );
            break;
          case ValueMetaInterface.TYPE_NUMBER:
            pentahoData = avroData;
            break;
          case ValueMetaInterface.TYPE_BIGNUMBER:
            pentahoData = BigDecimal.valueOf( avroData );
            break;
          case ValueMetaInterface.TYPE_TIMESTAMP:
            pentahoData = new Timestamp( avroData.longValue() );
            break;
          case ValueMetaInterface.TYPE_DATE:
            pentahoData = new Date( avroData.longValue() );
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            pentahoData = ( avroData == 0 ? Boolean.FALSE : Boolean.TRUE );
            break;
        }
      } catch ( Exception e ) {
        // If unable to do the type conversion just ignore. null will be returned.
      }
    }
    return pentahoData;
  }

  private Object convertToPentahoType( int pentahoType, Boolean avroData ) {
    Object pentahoData = null;
    if ( avroData != null ) {
      try {
        switch ( pentahoType ) {
          case ValueMetaInterface.TYPE_STRING:
            pentahoData = avroData.toString();
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            pentahoData = avroData;
            break;
          case ValueMetaInterface.TYPE_INTEGER:
            pentahoData = avroData ? new Long( 1 ) : new Long( 0 );
            break;
          case ValueMetaInterface.TYPE_NUMBER:
            pentahoData = avroData ? new Double( 1 ) : new Double( 0 );
            break;
          case ValueMetaInterface.TYPE_BIGNUMBER:
            pentahoData = avroData ? new BigDecimal( 1 ) : new BigDecimal( 0 );
            break;
        }
      } catch ( Exception e ) {
        // If unable to do the type conversion just ignore. null will be returned.
      }
    }
    return pentahoData;
  }

  private Object convertToPentahoType( int pentahoType, String avroData, IAvroInputField avroInputField ) {
    Object pentahoData = null;
    if ( avroData != null ) {
      try {
        switch ( pentahoType ) {
          case ValueMetaInterface.TYPE_INET:
            pentahoData = InetAddress.getByName( avroData.toString() );
            break;
          case ValueMetaInterface.TYPE_STRING:
            pentahoData = avroData;
            break;
          case ValueMetaInterface.TYPE_INTEGER:
            pentahoData = Long.parseLong( avroData );
            break;
          case ValueMetaInterface.TYPE_NUMBER:
            pentahoData = Double.parseDouble( avroData );
            break;
          case ValueMetaInterface.TYPE_BIGNUMBER:
            pentahoData = new BigDecimal( avroData );
            break;
          case ValueMetaInterface.TYPE_TIMESTAMP:
            pentahoData = new Timestamp( Long.parseLong( avroData ) );
            break;
          case ValueMetaInterface.TYPE_DATE:
            String dateFormatStr = avroInputField.getStringFormat();
            if ( ( dateFormatStr == null ) || ( dateFormatStr.trim().length() == 0 ) ) {
              dateFormatStr = ValueMetaBase.DEFAULT_DATE_FORMAT_MASK;
            }
            SimpleDateFormat datePattern = new SimpleDateFormat( dateFormatStr );
            pentahoData = datePattern.parse( avroData );
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            pentahoData = Boolean.valueOf( "Y".equalsIgnoreCase( avroData ) || "TRUE".equalsIgnoreCase( avroData )
              || "YES".equalsIgnoreCase( avroData ) || "1".equals( avroData ) );
            break;
        }
      } catch ( Exception e ) {
        // If unable to do the type conversion just ignore. null will be returned.
      }
    }
    return pentahoData;
  }

  private Object convertToPentahoType( int pentahoType, byte[] avroData, Schema field ) {
    Object pentahoData = null;
    if ( avroData != null ) {
      try {
        switch ( pentahoType ) {
          case ValueMetaInterface.TYPE_BINARY:
            pentahoData = avroData;
            break;
        }
      } catch ( Exception e ) {
        // If unable to do the type conversion just ignore. null will be returned.
      }
    }
    return pentahoData;
  }

  private boolean isLegacySchema( Schema schema ) {
    if ( schema.getFields().size() > 0 ) {
      Schema.Field field = schema.getFields().get( 0 );
      return field != null && field.name() != null && field.name()
        .contains( PentahoAvroInputFormat.FieldName.FIELDNAME_DELIMITER );
    } else {
      return false;
    }
  }
}
