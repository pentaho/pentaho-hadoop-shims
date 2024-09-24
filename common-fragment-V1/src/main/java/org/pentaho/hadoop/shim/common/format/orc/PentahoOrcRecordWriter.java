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
package org.pentaho.hadoop.shim.common.format.orc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.pentaho.di.core.RowMetaAndData;
import org.apache.hadoop.conf.Configuration;
import org.pentaho.di.core.exception.KettleValueException;
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
import org.pentaho.hadoop.shim.api.format.IOrcOutputField;
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat;
import org.pentaho.hadoop.shim.common.format.S3NCredentialUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by tkafalas on 11/3/2017.
 */
public class PentahoOrcRecordWriter implements IPentahoOutputFormat.IPentahoRecordWriter {
  private final TypeDescription schema;
  protected VectorizedRowBatch batch;
  protected int batchRowNumber;
  protected Writer writer;
  protected final IValueMetaConverter valueMetaConverter = new ValueMetaConverter();
  private final String dateFormatString = ValueMetaBase.DEFAULT_DATE_FORMAT_MASK; //TODO: Get this from UI
  private SimpleDateFormat datePattern = new SimpleDateFormat( dateFormatString );
  protected RowMeta outputRowMeta = new RowMeta();
  protected RowMetaAndData outputRowMetaAndData;
  protected static final Logger logger = LogManager.getLogger( PentahoOrcRecordWriter.class );
  protected List<? extends IOrcOutputField> fields;

  public PentahoOrcRecordWriter( List<? extends IOrcOutputField> fields, TypeDescription schema, String filePath,
                                 Configuration conf ) {
    this.fields = fields;
    this.schema = schema;
    final AtomicInteger fieldNumber = new AtomicInteger();  //Mutable field count
    fields.forEach( field -> setOutputMeta( fieldNumber, field ) );
    outputRowMetaAndData = new RowMetaAndData( outputRowMeta, new Object[ fieldNumber.get() ] );

    try {
      S3NCredentialUtils util = new S3NCredentialUtils();
      util.applyS3CredentialsToHadoopConfigurationIfNecessary( filePath, conf );
      Path outputFile = new Path( S3NCredentialUtils.scrubFilePathIfNecessary( filePath ) );
      writer = OrcFile.createWriter( outputFile,
        OrcFile.writerOptions( conf )
          .setSchema( schema ) );
      batch = schema.createRowBatch();
    } catch ( IOException e ) {
      logger.error( e );
    }

    //Write the addition metadata for the fields
    // new OrcMetaDataWriter( writer ).write( fields );
  }

  protected void setOutputMeta( AtomicInteger fieldNumber, IOrcOutputField field ) {
    outputRowMeta.addValueMeta( getValueMetaInterface( field.getPentahoFieldName(), field.getOrcType().getPdiType() ) );
    fieldNumber.getAndIncrement();
  }

  @Override public void write( RowMetaAndData row ) throws Exception {
    final AtomicInteger fieldNumber = new AtomicInteger();  //Mutable field count
    batchRowNumber = batch.size++;

    fields.forEach( field -> setFieldValue( fieldNumber, field, row ) );
    if ( batch.size == batch.getMaxSize() - 1 ) {
      writer.addRowBatch( batch );
      batch.reset();
    }
  }

  private void setFieldValue( AtomicInteger fieldNumber, IOrcOutputField field,
                              RowMetaAndData rowMetaAndData ) {
    int fieldNo = fieldNumber.getAndIncrement();
    ColumnVector columnVector = batch.cols[ fieldNo ];
    int rowMetaIndex = rowMetaAndData.getRowMeta().indexOfValue( field.getPentahoFieldName() );

    if ( rowMetaAndData.getData()[ rowMetaIndex ] == null ) {
      if ( field.getAllowNull() ) {
        columnVector.isNull[ batchRowNumber ] = true;
        columnVector.noNulls = false;
        return;
      }
    }

    columnVector.isNull[ batchRowNumber ] = false;
    int inlineType = rowMetaAndData.getRowMeta().getValueMeta( rowMetaIndex ).getType();
    Object inlineValue = rowMetaAndData.getData()[ rowMetaIndex ];
    Object setValue = null;

    try {
      setValue = valueMetaConverter
        .convertFromSourceToTargetDataType( inlineType, field.getOrcType().getPdiType(), inlineValue );
    } catch ( ValueMetaConversionException e ) {
      logger.error( e );
    }
    //Set the final converted value
    outputRowMetaAndData.getData()[ rowMetaIndex ] = setValue;

    switch ( field.getOrcType() ) {
      case BOOLEAN:
        try {
          ( (LongColumnVector) columnVector ).vector[ batchRowNumber ] =
            rowMetaAndData.getBoolean( field.getPentahoFieldName(),
              field.getDefaultValue() != null ? Boolean.valueOf( field.getDefaultValue() ) : false ) ? 1L : 0L;
        } catch ( KettleValueException e ) {
          logger.error( e );
        }
        break;
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        try {
          ( (LongColumnVector) columnVector ).vector[ batchRowNumber ] =
            rowMetaAndData.getInteger( field.getPentahoFieldName(),
              field.getDefaultValue() != null ? Long.valueOf( field.getDefaultValue() ) : 0 );
        } catch ( KettleValueException e ) {
          logger.error( e );
        }
        break;
      case BINARY:
        try {
          setBytesColumnVector( ( (BytesColumnVector) columnVector ),
            rowMetaAndData.getBinary( field.getPentahoFieldName(),
              field.getDefaultValue() != null ? field.getDefaultValue().getBytes() : new byte[ 0 ] ) );
        } catch ( KettleValueException e ) {
          logger.error( e );
        }
        break;
      case FLOAT:
      case DOUBLE:
        try {
          double number = rowMetaAndData.getNumber( field.getPentahoFieldName(),
            field.getDefaultValue() != null ? new Double( field.getDefaultValue() ) : new Double( 0 ) );
          number = applyScale( number, field );
          ( (DoubleColumnVector) columnVector ).vector[ batchRowNumber ] = number;
        } catch ( KettleValueException e ) {
          logger.error( e );
        }
        break;
      case DECIMAL:
        try {
          ( (DecimalColumnVector) columnVector ).vector[ batchRowNumber ] = new HiveDecimalWritable( HiveDecimal.create(
            rowMetaAndData.getBigNumber( field.getPentahoFieldName(),
              field.getDefaultValue() != null ? new BigDecimal( field.getDefaultValue() ) : new BigDecimal( 0 ) ) ) );
        } catch ( KettleValueException e ) {
          logger.error( e );
        }
        break;
      case CHAR:
      case VARCHAR:
      case STRING:
        try {
          setBytesColumnVector( ( (BytesColumnVector) columnVector ),
            rowMetaAndData.getString( field.getPentahoFieldName(),
              field.getDefaultValue() != null ? field.getDefaultValue() : "" ) );
        } catch ( KettleValueException e ) {
          logger.error( e );
        }
        break;
      case DATE:
        try {
          String conversionMask = rowMetaAndData.getValueMeta( rowMetaIndex ).getConversionMask();
          if ( conversionMask == null ) {
            conversionMask = ValueMetaBase.DEFAULT_DATE_PARSE_MASK;
          }
          DateFormat dateFormat = new SimpleDateFormat( conversionMask );
          Date defaultDate =
            field.getDefaultValue() != null ? dateFormat.parse( field.getDefaultValue() ) : new Date( 0 );
          Date date = rowMetaAndData.getDate( field.getPentahoFieldName(), defaultDate );
          ( (LongColumnVector) columnVector ).vector[ batchRowNumber ] =
            getOrcDate( date, rowMetaAndData.getValueMeta( rowMetaIndex ).getDateFormatTimeZone() );
        } catch ( KettleValueException | ParseException e ) {
          logger.error( e );
        }
        break;
      case TIMESTAMP:
        try {
          String conversionMask = rowMetaAndData.getValueMeta( rowMetaIndex ).getConversionMask();
          if ( conversionMask == null ) {
            conversionMask = ValueMetaBase.DEFAULT_DATE_PARSE_MASK;
          }
          DateFormat dateFormat = new SimpleDateFormat( conversionMask );
          ( (TimestampColumnVector) columnVector ).set( batchRowNumber,
            new Timestamp( rowMetaAndData.getDate( field.getPentahoFieldName(),
              field.getDefaultValue() != null ? ( dateFormat.parse( field.getDefaultValue() ) ) : new Date( 0 ) )
              .getTime() ) );
        } catch ( KettleValueException | ParseException e ) {
          logger.error( e );
        }
        break;
      default:
        throw new RuntimeException(
          "Field: " + field.getDefaultValue() + "  Undefined type: " + field.getOrcType().getName() );
    }
  }

  private double applyScale( double number, IOrcOutputField outputField ) {
    if ( outputField.getScale() > 0 ) {
      BigDecimal bd = new BigDecimal( number );
      bd = bd.setScale( outputField.getScale(), BigDecimal.ROUND_HALF_UP );
      number = bd.doubleValue();
    }
    return number;
  }


  protected int getOrcDate( Date date, TimeZone timeZone ) {
    if ( timeZone == null ) {
      timeZone = TimeZone.getDefault();
    }
    LocalDate rowDate = date.toInstant().atZone( timeZone.toZoneId() ).toLocalDate();
    return Math.toIntExact( ChronoUnit.DAYS.between( LocalDate.ofEpochDay( 0 ), rowDate ) );
  }

  private void setBytesColumnVector( BytesColumnVector bytesColumnVector, String value ) {
    if ( value == null ) {
      setBytesColumnVector( bytesColumnVector, new byte[ 0 ] );
    } else {
      setBytesColumnVector( bytesColumnVector, value.getBytes() );
    }
  }

  protected void setBytesColumnVector( BytesColumnVector bytesColumnVector, byte[] value ) {
    bytesColumnVector.vector[ batchRowNumber ] = value;
    bytesColumnVector.start[ batchRowNumber ] = 0;
    bytesColumnVector.length[ batchRowNumber ] = value.length;
  }

  @Override public void close() throws IOException {
    if ( batch.size > 0 ) {
      writer.addRowBatch( batch );
    }
    writer.close();
  }

  private ByteBuffer toByteBuffer( int i ) throws UnsupportedEncodingException {
    return toByteBuffer( String.valueOf( i ) );
  }

  private ByteBuffer toByteBuffer( String s ) throws UnsupportedEncodingException {
    return ByteBuffer.wrap( s.getBytes( "UTF-8" ) );
  }

  private ByteBuffer toByteBuffer( boolean b ) throws UnsupportedEncodingException {
    return ByteBuffer.wrap( String.valueOf( b ).getBytes( "UTF-8" ) );
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
