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
import org.apache.log4j.Logger;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.pentaho.di.core.RowMetaAndData;
import org.apache.hadoop.conf.Configuration;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by tkafalas on 11/3/2017.
 */
public class PentahoOrcRecordWriter implements IPentahoOutputFormat.IPentahoRecordWriter {
  private final SchemaDescription schemaDescription;
  private final TypeDescription schema;
  Configuration conf;
  VectorizedRowBatch batch;
  int batchRowNumber;
  Writer writer;
  private static final Logger logger = Logger.getLogger( PentahoOrcRecordWriter.class );

  public int batchMaxSize = 10000;

  public PentahoOrcRecordWriter( SchemaDescription schemaDescription, TypeDescription schema, String filePath,
                                 Configuration conf ) {
    this.schemaDescription = schemaDescription;
    this.schema = schema;

    try {
      writer = OrcFile.createWriter( new Path( filePath ),
        OrcFile.writerOptions( conf )
          .setSchema( schema ) );
      batch = schema.createRowBatch();
    } catch ( IOException e ) {
      e.printStackTrace();
    }

    //Write the addition metadata for the fields
    new OrcMetaDataWriter( writer ).write( schemaDescription );
  }

  @Override public void write( RowMetaAndData row ) throws Exception {
    final AtomicInteger fieldNumber = new AtomicInteger();  //Mutable field count
    batchRowNumber = batch.size++;
    schemaDescription.forEach( field -> setFieldValue( fieldNumber, field, row ) );
    if ( batch.size == batchMaxSize ) {
      writer.addRowBatch( batch );
      batch.reset();
    }
  }

  private void setFieldValue( AtomicInteger fieldNumber, SchemaDescription.Field field,
                              RowMetaAndData rowMetaAndData ) {
    int fieldNo = fieldNumber.getAndIncrement();
    ColumnVector columnVector = batch.cols[ fieldNo ];
    int rowMetaIndex = rowMetaAndData.getRowMeta().indexOfValue( field.pentahoFieldName );

    if ( rowMetaAndData.getData()[ rowMetaIndex ] == null ) {
      if ( field.allowNull ) {
        columnVector.isNull[ batchRowNumber ] = true;
        columnVector.noNulls = false;
        return;
      }
    }
    columnVector.isNull[ batchRowNumber ] = false;
    switch ( field.pentahoValueMetaType ) {
      case ValueMetaInterface.TYPE_NUMBER:
        try {
          ( (DoubleColumnVector) columnVector ).vector[ batchRowNumber ] =
            rowMetaAndData.getNumber( field.pentahoFieldName,
              field.defaultValue != null ? Double.valueOf( field.defaultValue ) : 0 );
        } catch ( KettleValueException e ) {
          e.printStackTrace();
        }
        break;
      case ValueMetaInterface.TYPE_BIGNUMBER:
        try {
          ( (DecimalColumnVector) columnVector ).vector[ batchRowNumber ] = new HiveDecimalWritable( HiveDecimal.create(
            rowMetaAndData.getBigNumber( field.pentahoFieldName,
              field.defaultValue != null ? new BigDecimal( field.defaultValue ) : new BigDecimal( 0 ) ) ) );
        } catch ( KettleValueException e ) {
          e.printStackTrace();
        }
        break;
      case ValueMetaInterface.TYPE_INET:
        try {
          setBytesColumnVector( ( (BytesColumnVector) columnVector ),
            rowMetaAndData.getString( field.pentahoFieldName,
              field.defaultValue != null ? field.defaultValue : InetAddress.getLocalHost().toString() ) );
        } catch ( KettleValueException | UnknownHostException e ) {
          e.printStackTrace();
        }
        break;
      case ValueMetaInterface.TYPE_STRING:
        try {
          setBytesColumnVector( ( (BytesColumnVector) columnVector ),
            rowMetaAndData.getString( field.pentahoFieldName, field.defaultValue != null ? field.defaultValue : "" ) );
        } catch ( KettleValueException e ) {
          e.printStackTrace();
        }
        break;
      case ValueMetaInterface.TYPE_BOOLEAN:
        try {
          ( (LongColumnVector) columnVector ).vector[ batchRowNumber ] =
            rowMetaAndData.getBoolean( field.pentahoFieldName,
              field.defaultValue != null ? Boolean.valueOf( field.defaultValue ) : false ) ? 1L : 0L;
        } catch ( KettleValueException e ) {
          e.printStackTrace();
        }
        break;
      case ValueMetaInterface.TYPE_INTEGER:
        try {
          ( (LongColumnVector) columnVector ).vector[ batchRowNumber ] =
            rowMetaAndData.getInteger( field.pentahoFieldName,
              field.defaultValue != null ? Long.parseLong( field.defaultValue ) : 0 );
        } catch ( KettleValueException e ) {
          e.printStackTrace();
        }
        break;
      case ValueMetaInterface.TYPE_SERIALIZABLE:
        try {
          setBytesColumnVector( ( (BytesColumnVector) columnVector ),
            rowMetaAndData.getBinary( field.pentahoFieldName,
              field.defaultValue != null ? field.defaultValue.getBytes() : new byte[ 0 ] ) );
        } catch ( KettleValueException e ) {
          e.printStackTrace();
        }
        break;
      case ValueMetaInterface.TYPE_BINARY:
        try {
          setBytesColumnVector( ( (BytesColumnVector) columnVector ),
            rowMetaAndData.getBinary( field.pentahoFieldName,
              field.defaultValue != null ? field.defaultValue.getBytes() : new byte[ 0 ] ) );
        } catch ( KettleValueException e ) {
          e.printStackTrace();
        }
        break;
      case ValueMetaInterface.TYPE_DATE:
        try {
          ( (LongColumnVector) columnVector ).vector[ batchRowNumber ] =
            rowMetaAndData.getDate( field.pentahoFieldName,
              field.defaultValue != null ? new Date( Integer.valueOf( field.defaultValue ) ) : new Date( 0 ) )
              .getTime();
        } catch ( KettleValueException e ) {
          e.printStackTrace();
        }
        break;
      case ValueMetaInterface.TYPE_TIMESTAMP:
        try {
          ( (TimestampColumnVector) columnVector ).set( batchRowNumber,
            new Timestamp( rowMetaAndData.getDate( field.pentahoFieldName,
              field.defaultValue != null ? new Date( Integer.valueOf( field.defaultValue ) ) : new Date( 0 ) )
              .getTime() ) );
        } catch ( KettleValueException e ) {
          e.printStackTrace();
        }
        break;
      default:
        throw new RuntimeException(
          "Field: " + field.formatFieldName + "  Undefined type: " + field.pentahoValueMetaType );
    }

  }

  private void setBytesColumnVector( BytesColumnVector bytesColumnVector, String value ) {
    if ( value == null ) {
      setBytesColumnVector( bytesColumnVector, new byte[ 0 ] );
    } else {
      setBytesColumnVector( bytesColumnVector, value.getBytes() );
    }
  }

  private void setBytesColumnVector( BytesColumnVector bytesColumnVector, byte[] value ) {
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
}
