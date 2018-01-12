/*******************************************************************************
 *
 * Pentaho Big Data
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

package org.pentaho.hadoop.shim.common.format.avro;

import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.AvroSpec;
import org.pentaho.hadoop.shim.api.format.IAvroInputField;
import org.pentaho.hadoop.shim.api.format.IPentahoAvroInputFormat;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Created by rmansoor on 10/5/2017.
 */
public class PentahoAvroRecordReader implements IPentahoAvroInputFormat.IPentahoRecordReader {

  private final DataFileStream<GenericRecord> nativeAvroRecordReader;
  private final Schema avroSchema;
  private final List<? extends IAvroInputField> fields;

  public PentahoAvroRecordReader( DataFileStream<GenericRecord> nativeAvroRecordReader,
                                  Schema avroSchema, List<? extends IAvroInputField> fields) {
    this.nativeAvroRecordReader = nativeAvroRecordReader;
    this.avroSchema = avroSchema;
    this.fields = fields;
  }

  @Override public void close() throws IOException {
    nativeAvroRecordReader.close();
  }

  @Override public Iterator<RowMetaAndData> iterator() {
    return new Iterator<RowMetaAndData>() {

      @Override public boolean hasNext() {
        return nativeAvroRecordReader.hasNext();
      }

      @Override public RowMetaAndData next() {
        return getRowMetaAndData(  nativeAvroRecordReader.next() );
      }
    };
  }

  @VisibleForTesting
  public RowMetaAndData getRowMetaAndData( GenericRecord avroRecord ) {
    RowMetaAndData rowMetaAndData = new RowMetaAndData();
    for ( IAvroInputField metaField : fields ) {
      Schema.Field avroField = avroSchema.getField( metaField.getAvroFieldName() );

      if ( avroField == null ) {
        return null;
      }

      if ( metaField != null ) {
        AvroSpec.DataType avroDataType = null;

        String logicalType  = avroField.getProp(AvroSpec.LOGICAL_TYPE);
        for (AvroSpec.DataType tmpType : AvroSpec.DataType.values()) {
          if (!tmpType.isPrimitiveType() && tmpType.getType().equals( logicalType )) {
            avroDataType = tmpType;
            break;
          }
        }

        if (avroDataType == null) {
          Schema.Type primitiveAvroType = null;
          if ( avroField.schema().getType().equals( Schema.Type.UNION ) ) {
            List<Schema> schemas = avroField.schema().getTypes();
            for ( Schema s: schemas ) {
              if ( !s.getName().equalsIgnoreCase( "null" ) ) {
                primitiveAvroType = s.getType();
                break;
              }
            }
          } else {
            primitiveAvroType = avroField.schema().getType();
          }

          switch (primitiveAvroType) {
            case INT:
              avroDataType = AvroSpec.DataType.INTEGER;
              break;
            case LONG:
              avroDataType = AvroSpec.DataType.LONG;
              break;
            case BYTES:
              avroDataType = AvroSpec.DataType.BYTES;
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

        }

        Object avroData = avroRecord.get( metaField.getAvroFieldName() );
        Object pentahoData = null;

        if (avroData != null) {
          if ( avroData instanceof Utf8 ) {
            avroData = avroData.toString();
          }

          int pentahoType = metaField.getPentahoType();
          switch ( avroDataType ) {
            case BOOLEAN:
              pentahoData = convertToPentahoType(pentahoType, (Boolean)avroData);
              break;
            case DATE:
              pentahoData = convertToPentahoType(pentahoType, (Integer)avroData);
              break;
            case FLOAT:
              pentahoData = convertToPentahoType(pentahoType, (Float)avroData);
              break;
            case DOUBLE:
              pentahoData = convertToPentahoType(pentahoType, (Double)avroData);
              break;
            case LONG:
              pentahoData = convertToPentahoType(pentahoType, (Long)avroData);
              break;
            case DECIMAL:
              pentahoData = convertToPentahoType(pentahoType, (ByteBuffer)avroData, avroField);
              break;
            case INTEGER:
              pentahoData = convertToPentahoType(pentahoType, (Integer)avroData);
              break;
            case STRING:
              pentahoData = convertToPentahoType(pentahoType, (String)avroData);
              break;
            case BYTES:
              pentahoData = convertToPentahoType(pentahoType, (ByteBuffer)avroData, avroField);
              break;
            case TIMESTAMP_MILLIS:
              pentahoData = convertToPentahoType(pentahoType, (Long)avroData);
              break;
          }
        }
        rowMetaAndData.addValue( metaField.getPentahoFieldName(), metaField.getPentahoType(),  pentahoData );
      }
    }
    return rowMetaAndData;
  }

  private Object convertToPentahoType(int pentahoType, Float avroData) {
    Object pentahoData = null;
    if (avroData != null) {
      try {
        switch ( pentahoType ) {
          case ValueMetaInterface.TYPE_STRING:
            pentahoData = avroData.toString();
            break;
          case ValueMetaInterface.TYPE_INTEGER:
             pentahoData = new Long(avroData.longValue());
            break;
          case ValueMetaInterface.TYPE_NUMBER:
            pentahoData = Double.parseDouble(avroData.toString());
            break;
          case ValueMetaInterface.TYPE_BIGNUMBER:
            new BigDecimal(avroData.toString());
            break;
          case ValueMetaInterface.TYPE_TIMESTAMP:
            pentahoData = new Timestamp( avroData.longValue() );
            break;
          case ValueMetaInterface.TYPE_DATE:
            Instant instant = Instant.EPOCH.plus( avroData.longValue(), ChronoUnit.DAYS );
            Calendar calendar = Calendar.getInstance();
            calendar.setTime( Date.from(instant) );
            calendar.set( Calendar.HOUR_OF_DAY, 0 );
            calendar.set( Calendar.MINUTE, 0 );
            calendar.set( Calendar.SECOND, 0 );
            calendar.set( Calendar.MILLISECOND, 0 );
            pentahoData = calendar.getTime();
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            pentahoData = (avroData == 0 ? Boolean.FALSE : Boolean.TRUE);
            break;
        }
      } catch ( Exception e ) {
        // If unable to do the type conversion just ignore. null will be returned.
      }
    }
    return pentahoData;
  }

  private Object convertToPentahoType(int pentahoType, ByteBuffer avroData, Schema.Field field) {
    Object pentahoData = null;
    if (avroData != null) {
      try {
        switch ( pentahoType ) {
          case ValueMetaInterface.TYPE_BIGNUMBER:
            Conversions.DecimalConversion converter = new Conversions.DecimalConversion();
            Object precision = field.getObjectProp( "precision" );
            Object scale = field.getObjectProp( "scale" );
            LogicalTypes.Decimal decimalType = LogicalTypes.decimal( Integer.parseInt( precision.toString() ), Integer.parseInt( scale.toString() ) );
            pentahoData = converter.fromBytes( avroData, avroSchema,   decimalType);
            break;
          case ValueMetaInterface.TYPE_BINARY:
            pentahoData = new byte[avroData.remaining()];
            avroData.get((byte[])pentahoData);
            break;
        }
      } catch ( Exception e ) {
        // If unable to do the type conversion just ignore. null will be returned.
      }
    }
    return pentahoData;
  }


  private Object convertToPentahoType(int pentahoType, Long avroData) {
    Object pentahoData = null;
    if (avroData != null) {
      try {
        switch ( pentahoType ) {
          case ValueMetaInterface.TYPE_STRING:
            pentahoData = avroData.toString();
            break;
          case ValueMetaInterface.TYPE_INTEGER:
            pentahoData = avroData.longValue();
            break;
          case ValueMetaInterface.TYPE_NUMBER:
            pentahoData = new Double(avroData.doubleValue());
            break;
          case ValueMetaInterface.TYPE_BIGNUMBER:
            pentahoData = BigDecimal.valueOf( avroData );
            break;
          case ValueMetaInterface.TYPE_TIMESTAMP:
            pentahoData = new Timestamp( avroData );
            break;
          case ValueMetaInterface.TYPE_DATE:
            Instant instant = Instant.EPOCH.plus( avroData, ChronoUnit.DAYS );
            Calendar calendar = Calendar.getInstance();
            calendar.setTime( Date.from(instant) );
            calendar.set( Calendar.HOUR_OF_DAY, 0 );
            calendar.set( Calendar.MINUTE, 0 );
            calendar.set( Calendar.SECOND, 0 );
            calendar.set( Calendar.MILLISECOND, 0 );
            pentahoData = calendar.getTime();
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            pentahoData = (avroData == 0 ? Boolean.FALSE : Boolean.TRUE);
            break;
        }
      } catch ( Exception e ) {
        // If unable to do the type conversion just ignore. null will be returned.
      }
    }
    return pentahoData;
  }

  private Object convertToPentahoType(int pentahoType, Integer avroData) {
    Object pentahoData = null;
    if (avroData != null) {
      try {
        switch ( pentahoType ) {
          case ValueMetaInterface.TYPE_STRING:
            pentahoData = avroData.toString();
            break;
          case ValueMetaInterface.TYPE_INTEGER:
            pentahoData = new Long(avroData.longValue());
            break;
          case ValueMetaInterface.TYPE_NUMBER:
            pentahoData = new Double(avroData.doubleValue());
            break;
          case ValueMetaInterface.TYPE_BIGNUMBER:
            pentahoData = BigDecimal.valueOf( avroData );
            break;
          case ValueMetaInterface.TYPE_TIMESTAMP:
            pentahoData = new Timestamp( avroData );
            break;
          case ValueMetaInterface.TYPE_DATE:
            Instant instant = Instant.EPOCH.plus( avroData.longValue(), ChronoUnit.DAYS );
            Calendar calendar = Calendar.getInstance();
            calendar.setTime( Date.from(instant) );
            calendar.set( Calendar.HOUR_OF_DAY, 0 );
            calendar.set( Calendar.MINUTE, 0 );
            calendar.set( Calendar.SECOND, 0 );
            calendar.set( Calendar.MILLISECOND, 0 );
            pentahoData = calendar.getTime();
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            pentahoData = (avroData == 0 ? Boolean.FALSE : Boolean.TRUE);
            break;
        }
      } catch ( Exception e ) {
        // If unable to do the type conversion just ignore. null will be returned.
      }
    }
    return pentahoData;
  }

  private Object convertToPentahoType(int pentahoType, Double avroData) {
    Object pentahoData = null;
    if (avroData != null) {
      try {
        switch ( pentahoType ) {
          case ValueMetaInterface.TYPE_STRING:
            pentahoData = avroData.toString();
            break;
          case ValueMetaInterface.TYPE_INTEGER:
            pentahoData = new Long(avroData.longValue());
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
            Instant instant = Instant.EPOCH.plus( avroData.longValue(), ChronoUnit.DAYS );
            Calendar calendar = Calendar.getInstance();
            calendar.setTime( Date.from(instant) );
            calendar.set( Calendar.HOUR_OF_DAY, 0 );
            calendar.set( Calendar.MINUTE, 0 );
            calendar.set( Calendar.SECOND, 0 );
            calendar.set( Calendar.MILLISECOND, 0 );
            pentahoData = calendar.getTime();
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            pentahoData = (avroData == 0 ? Boolean.FALSE : Boolean.TRUE);
            break;
        }
      } catch ( Exception e ) {
        // If unable to do the type conversion just ignore. null will be returned.
      }
    }
    return pentahoData;
  }

  private Object convertToPentahoType(int pentahoType, Boolean avroData) {
    Object pentahoData = null;
    if (avroData != null) {
      try {
        switch ( pentahoType ) {
          case ValueMetaInterface.TYPE_STRING:
            pentahoData = avroData.toString();
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            pentahoData = avroData;
            break;
          case ValueMetaInterface.TYPE_INTEGER:
            pentahoData = avroData ? new Long(1 ) : new Long( 0 );
            break;
          case ValueMetaInterface.TYPE_NUMBER:
            pentahoData = avroData ? new Double(1 ) : new Double( 0 );
            break;
          case ValueMetaInterface.TYPE_BIGNUMBER:
            pentahoData = avroData  ? new BigDecimal(1 ) : new BigDecimal( 0 );
            break;
        }
      } catch ( Exception e ) {
        // If unable to do the type conversion just ignore. null will be returned.
      }
    }
    return pentahoData;
  }

  private Object convertToPentahoType(int pentahoType, String avroData) {
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
            Instant instant = Instant.EPOCH.plus( Long.parseLong( avroData ), ChronoUnit.DAYS );
            Calendar calendar = Calendar.getInstance();
            calendar.setTime( Date.from(instant) );
            calendar.set( Calendar.HOUR_OF_DAY, 0 );
            calendar.set( Calendar.MINUTE, 0 );
            calendar.set( Calendar.SECOND, 0 );
            calendar.set( Calendar.MILLISECOND, 0 );
            pentahoData = calendar.getTime();
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            pentahoData = Boolean.parseBoolean( avroData );
            break;
        }
      } catch ( Exception e ) {
        // If unable to do the type conversion just ignore. null will be returned.
      }
    }
    return pentahoData;
  }

}
