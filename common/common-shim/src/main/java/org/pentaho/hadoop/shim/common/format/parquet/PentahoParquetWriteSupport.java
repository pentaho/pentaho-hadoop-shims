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
package org.pentaho.hadoop.shim.common.format.parquet;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.JulianFields;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;

//#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI" || shim_name=="mapr60" || shim_name=="cdh610"
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
//#endif
//#if shim_type=="CDH" && shim_name!="cdh610" || shim_type=="MAPR" && shim_name!="mapr60"
//$import parquet.hadoop.api.WriteSupport;
//$import parquet.io.api.Binary;
//$import parquet.io.api.RecordConsumer;
//$import parquet.schema.MessageType;
//$import parquet.schema.OriginalType;
//$import parquet.schema.PrimitiveType;
//$import parquet.schema.Type;
//$import parquet.schema.Types;
//#endif

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.hadoop.shim.api.format.IParquetOutputField;
import org.pentaho.hadoop.shim.api.format.ParquetSpec;

public class PentahoParquetWriteSupport extends WriteSupport<RowMetaAndData> {
  private RecordConsumer consumer;
  private List<? extends IParquetOutputField> outputFields;
  byte[] timestampBuffer = new byte[12];

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
    String conversionMask = null;
    String defaultValue = null;
    DateFormat dateFormat = null;
    TimeZone timeZone = null;
    LocalDate localDate = null;

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
          BigDecimal bigDecimal;
          consumer.startField( field.getFormatFieldName(), index );
          switch ( field.getParquetType() ) {
            case FLOAT:
              consumer.addFloat( applyScale( Float.parseFloat( field.getDefaultValue() ), field ) );
              break;
            case DOUBLE:
              consumer.addDouble( applyScale( Double.parseDouble( field.getDefaultValue() ), field ) );
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
            case INT_96:
              Date date = null;
              defaultValue = field.getDefaultValue();
              conversionMask = ( vmi.getConversionMask() == null ) ? ValueMetaBase.DEFAULT_DATE_PARSE_MASK : vmi.getConversionMask();
              dateFormat = new SimpleDateFormat( conversionMask );
              try {
                date = dateFormat.parse( defaultValue );
              } catch ( ParseException pe ) {
                date = new Date( 0 );
              }

              timeZone = vmi.getDateFormatTimeZone();
              if ( timeZone == null ) {
                timeZone = TimeZone.getDefault();
              }

              localDate = date.toInstant().atZone( timeZone.toZoneId() ).toLocalDate();
              long julianDay = JulianFields.JULIAN_DAY.getFrom( localDate );

              LocalDateTime ldt = LocalDateTime.ofInstant( date.toInstant(), timeZone.toZoneId() );
              ZonedDateTime zdt = ldt.atZone( timeZone.toZoneId() );
              ZonedDateTime utc = zdt.withZoneSameInstant( ZoneId.of( "UTC" ) );

              long timeOfDayNanos = utc.toInstant().toEpochMilli() * 1000000L - ( ( julianDay - ParquetSpec.JULIAN_DAY_OF_EPOCH ) * 24L * 60L * 60L * 1000L * 1000000L );
              ByteBuffer buf = ByteBuffer.wrap( timestampBuffer );

              buf.order( ByteOrder.LITTLE_ENDIAN ).putLong( timeOfDayNanos ).putInt( (int) julianDay );
              //#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI"
              consumer.addBinary( Binary.fromReusedByteArray( timestampBuffer ) );
              //#endif
              //#if shim_type=="CDH" || shim_type=="MAPR"
              //$     consumer.addBinary( Binary.fromByteArray( timestampBuffer ) );
              //#endif
              break;
            case DECIMAL:
              bigDecimal = new BigDecimal( field.getDefaultValue() );
              if ( bigDecimal != null ) {
                bigDecimal = bigDecimal.round( new MathContext( field.getPrecision(), RoundingMode.HALF_UP ) ).setScale( field.getScale(), RoundingMode.HALF_UP );
              }
              //#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI"
              consumer.addBinary( Binary.fromConstantByteArray( bigDecimal.unscaledValue().toByteArray() ) );
              //#endif
              //#if shim_type=="CDH" || shim_type=="MAPR"
              //$     consumer.addBinary( Binary.fromByteArray( bigDecimal.unscaledValue().toByteArray() ) );
              //#endif
              break;
            case DECIMAL_INT_32:
              bigDecimal = new BigDecimal( field.getDefaultValue() );
              if ( bigDecimal != null ) {
                bigDecimal = bigDecimal.round( new MathContext( field.getPrecision(), RoundingMode.HALF_UP ) ).setScale( field.getScale(), RoundingMode.HALF_UP );
              }
              consumer.addInteger( bigDecimal.unscaledValue().intValue() );
              break;
            case DECIMAL_INT_64:
              bigDecimal = new BigDecimal( field.getDefaultValue() );
              if ( bigDecimal != null ) {
                bigDecimal = bigDecimal.round( new MathContext( field.getPrecision(), RoundingMode.HALF_UP ) ).setScale( field.getScale(), RoundingMode.HALF_UP );
              }
              consumer.addLong( bigDecimal.unscaledValue().longValue() );
              break;
            case DATE:
              Date defaultDate = null;
              defaultValue = field.getDefaultValue();
              conversionMask = ( vmi.getConversionMask() == null ) ? ValueMetaBase.DEFAULT_DATE_PARSE_MASK : vmi.getConversionMask();
              dateFormat = new SimpleDateFormat( conversionMask );
              try {
                defaultDate = dateFormat.parse( defaultValue );
              } catch ( ParseException pe ) {
                // Do nothing
              }
              timeZone = vmi.getDateFormatTimeZone();
              if ( timeZone == null ) {
                timeZone = TimeZone.getDefault();
              }
              localDate = defaultDate.toInstant().atZone( timeZone.toZoneId() ).toLocalDate();
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
        consumer.addFloat( applyScale( (float) row.getNumber( fieldIndex, 0 ), field ) );
        break;
      case DOUBLE:
        consumer.addDouble( applyScale( row.getNumber( fieldIndex, 0 ), field ) );
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
      case INT_96:
        Date date = row.getDate( fieldIndex, null );
        timeZone = vmi.getDateFormatTimeZone();
        if ( timeZone == null ) {
          timeZone = TimeZone.getDefault();
        }

        localDate = date.toInstant().atZone( timeZone.toZoneId() ).toLocalDate();
        long julianDay = JulianFields.JULIAN_DAY.getFrom( localDate );

        LocalDateTime ldt = LocalDateTime.ofInstant( date.toInstant(), timeZone.toZoneId() );
        ZonedDateTime zdt = ldt.atZone( timeZone.toZoneId() );
        ZonedDateTime utc = zdt.withZoneSameInstant( ZoneId.of( "UTC" ) );

        long timeOfDayNanos = utc.toInstant().toEpochMilli() * 1000000L - ( ( julianDay - ParquetSpec.JULIAN_DAY_OF_EPOCH ) * 24L * 60L * 60L * 1000L * 1000000L );
        ByteBuffer buf = ByteBuffer.wrap( timestampBuffer );
        buf.order( ByteOrder.LITTLE_ENDIAN ).putLong( timeOfDayNanos ).putInt( (int) julianDay );

        //#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI"
        consumer.addBinary( Binary.fromReusedByteArray( timestampBuffer ) );
        //#endif
        //#if shim_type=="CDH" || shim_type=="MAPR"
        //$     consumer.addBinary( Binary.fromByteArray( timestampBuffer ) );
        //#endif
        break;
      case DECIMAL:
        BigDecimal bigDecimal = row.getBigNumber( fieldIndex, null );
        if ( bigDecimal != null ) {
          bigDecimal = bigDecimal.round( new MathContext( field.getPrecision(), RoundingMode.HALF_UP ) ).setScale( field.getScale(), RoundingMode.HALF_UP );
        }
        //#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI"
        consumer.addBinary( Binary.fromConstantByteArray( bigDecimal.unscaledValue().toByteArray() ) );
        //#endif
        //#if shim_type=="CDH" || shim_type=="MAPR"
        //$     consumer.addBinary( Binary.fromByteArray( bigDecimal.unscaledValue().toByteArray() ) );
        //#endif
        break;
      case DECIMAL_INT_32:
        bigDecimal = row.getBigNumber( fieldIndex, null );
        if ( bigDecimal != null ) {
          bigDecimal = bigDecimal.round( new MathContext( field.getPrecision(), RoundingMode.HALF_UP ) ).setScale( field.getScale(), RoundingMode.HALF_UP );
        }
        consumer.addInteger( bigDecimal.unscaledValue().intValue() );
        break;
      case DECIMAL_INT_64:
        bigDecimal = row.getBigNumber( fieldIndex, null );
        if ( bigDecimal != null ) {
          bigDecimal = bigDecimal.round( new MathContext( field.getPrecision(), RoundingMode.HALF_UP ) ).setScale( field.getScale(), RoundingMode.HALF_UP );
        }
        consumer.addLong( bigDecimal.unscaledValue().longValue() );
        break;
      case DATE:
        Date dateFromRow = row.getDate( fieldIndex, null );
        timeZone = vmi.getDateFormatTimeZone();
        if ( timeZone == null ) {
          timeZone = TimeZone.getDefault();
        }
        localDate = dateFromRow.toInstant().atZone( timeZone.toZoneId() ).toLocalDate();
        Integer dateInDays = Math.toIntExact( ChronoUnit.DAYS.between( LocalDate.ofEpochDay( 0 ), localDate ) );
        consumer.addInteger( dateInDays );
        break;
      default:
        throw new RuntimeException( "Undefined type: " + field.getPentahoType() );
    }

    consumer.endField( field.getFormatFieldName(), index );
  }

  private double applyScale( double number, IParquetOutputField outputField ) {
    if ( outputField.getScale() > 0 ) {
      BigDecimal bd = new BigDecimal( number );
      bd = bd.setScale( outputField.getScale(), BigDecimal.ROUND_HALF_UP );
      number = bd.doubleValue();
    }
    return number;
  }

  private float applyScale( float number, IParquetOutputField outputField ) {
    if ( outputField.getScale() > 0 ) {
      BigDecimal bd = new BigDecimal( number );
      bd = bd.setScale( outputField.getScale(), BigDecimal.ROUND_HALF_UP );
      number = bd.floatValue();
    }
    return number;
  }

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
      case INT_96:
        return new PrimitiveType( rep, PrimitiveType.PrimitiveTypeName.INT96, formatFieldName );
      case DATE:
        return new PrimitiveType( rep, PrimitiveType.PrimitiveTypeName.INT32, formatFieldName, OriginalType.DATE );
      case DECIMAL:
        if ( f.getAllowNull() ) {
          return Types.optional( PrimitiveType.PrimitiveTypeName.BINARY ).as( OriginalType.DECIMAL ).precision( f.getPrecision() ).scale( f.getScale() ).named( formatFieldName );
        } else {
          return Types.required( PrimitiveType.PrimitiveTypeName.BINARY ).as( OriginalType.DECIMAL ).precision( f.getPrecision() ).scale( f.getScale() ).named( formatFieldName );
        }
      case DECIMAL_INT_32:
        if ( f.getAllowNull() ) {
          return Types.optional( PrimitiveType.PrimitiveTypeName.INT32 ).as( OriginalType.DECIMAL ).precision( f.getPrecision() ).scale( f.getScale() ).named( formatFieldName );
        } else {
          return Types.required( PrimitiveType.PrimitiveTypeName.INT32 ).as( OriginalType.DECIMAL ).precision( f.getPrecision() ).scale( f.getScale() ).named( formatFieldName );
        }
      case DECIMAL_INT_64:
        if ( f.getAllowNull() ) {
          return Types.optional( PrimitiveType.PrimitiveTypeName.INT64 ).as( OriginalType.DECIMAL ).precision( f.getPrecision() ).scale( f.getScale() ).named( formatFieldName );
        } else {
          return Types.required( PrimitiveType.PrimitiveTypeName.INT64 ).as( OriginalType.DECIMAL ).precision( f.getPrecision() ).scale( f.getScale() ).named( formatFieldName );
        }
      case TIMESTAMP_MILLIS:
        return new PrimitiveType( rep, PrimitiveType.PrimitiveTypeName.INT64, formatFieldName, OriginalType.TIMESTAMP_MILLIS );
      default:
        throw new RuntimeException( "Unsupported output type: " + f.getParquetType() );
    }
  }
}
