/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Pentaho : http://www.pentaho.com
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

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.util.DateDetector;
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;
import org.pentaho.hadoop.shim.api.format.SchemaDescription.Field;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.Date;

/**
 * Created by tkafalas on 8/28/2017.
 */
public class PentahoAvroRecordWriter implements IPentahoOutputFormat.IPentahoRecordWriter {

  private final DataFileWriter<GenericRecord> nativeAvroRecordWriter;
  private final Schema schema;
  private final SchemaDescription schemaDescription;

  public PentahoAvroRecordWriter( DataFileWriter<GenericRecord> recordWriter, Schema schema, SchemaDescription schemaDescription ) {
    this.nativeAvroRecordWriter = recordWriter;
    this.schema = schema;
    this.schemaDescription = schemaDescription;
  }

  @Override
  public void write( RowMetaAndData row ) {
    RowMetaInterface rmi = row.getRowMeta();
    GenericRecord outputRecord = new GenericData.Record( schema );
    try {
      //Build the avro row
      for ( Field field : schemaDescription ) {
        int fieldMetaIndex = rmi.indexOfValue( field.pentahoFieldName );
        ValueMetaInterface vmi = rmi.getValueMeta( fieldMetaIndex );
        switch ( vmi.getType() ) {
          case ValueMetaInterface.TYPE_STRING:
            outputRecord.put( field.formatFieldName, row.getString( fieldMetaIndex, field.defaultValue ) );
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            outputRecord.put( field.formatFieldName, row.getBoolean( fieldMetaIndex, ValueMetaBase.convertStringToBoolean( field.defaultValue ) ) );
            break;
          case ValueMetaInterface.TYPE_INTEGER:
            outputRecord.put( field.formatFieldName, row.getInteger( fieldMetaIndex, Long.parseLong( field.defaultValue ) ) );
            break;
          case ValueMetaInterface.TYPE_NUMBER:
            outputRecord.put( field.formatFieldName, row.getNumber( fieldMetaIndex, Double.parseDouble( field.defaultValue ) ) );
            break;
          case ValueMetaInterface.TYPE_BIGNUMBER:
            if ( field.defaultValue != null ) {
              BigDecimal defaultBigDecimal = new BigDecimal( field.defaultValue );
              BigDecimal bigDecimal = row.getBigNumber( fieldMetaIndex, defaultBigDecimal );
              outputRecord.put( field.formatFieldName, bigDecimal.doubleValue() );
            } else {
              outputRecord.put( field.formatFieldName, row.getBigNumber( fieldMetaIndex, null ) );
            }
            break;
          case ValueMetaInterface.TYPE_SERIALIZABLE:
          case ValueMetaInterface.TYPE_BINARY:
            if ( field.defaultValue != null ) {
              outputRecord.put( field.formatFieldName, row.getBinary( fieldMetaIndex, vmi.getBinary( field.defaultValue ) ) );
            } else {
              outputRecord.put( field.formatFieldName, row.getBinary( fieldMetaIndex, new byte[0] ) );
            }
            break;
          case ValueMetaInterface.TYPE_TIMESTAMP:
          case ValueMetaInterface.TYPE_DATE:
            Date date = new Date();
            if ( field.defaultValue != null ) {
              try {
                date = DateDetector.getDateFromString( field.defaultValue );
              } catch ( ParseException e ) {
                //we do not able to get default value from field, we keep now as default value
              }
            }
            outputRecord.put( field.formatFieldName, row.getDate( fieldMetaIndex, date ).getTime() );
            break;
          default:
            break;
        }
      }
      //Now Append the row to the file
      nativeAvroRecordWriter.append( outputRecord );
    } catch ( IOException e ) {
      throw new IllegalArgumentException( "some exception while writing avro", e );
    } catch ( KettleValueException e ) {
      throw new IllegalArgumentException( "some exception while writing avro", e );
    }
  }

  @Override
  public void close() throws IOException {
    nativeAvroRecordWriter.close();
  }
}
