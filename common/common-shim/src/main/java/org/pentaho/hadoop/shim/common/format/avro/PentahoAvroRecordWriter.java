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
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Created by tkafalas on 8/28/2017.
 */
public class PentahoAvroRecordWriter implements IPentahoOutputFormat.IPentahoRecordWriter {
  private final DataFileWriter<GenericRecord> nativeAvroRecordWriter;
  private final Schema schema;

  public PentahoAvroRecordWriter( DataFileWriter<GenericRecord> recordWriter, Schema schema ) {
    this.nativeAvroRecordWriter = recordWriter;
    this.schema = schema;
  }

  @Override
  public void write( RowMetaAndData row ) {
    RowMetaInterface rmi = row.getRowMeta();
    GenericRecord outputRecord = new GenericData.Record( schema );

    try {
      //Build the avro row
      for ( int i = 0; i < rmi.getValueMetaList().size(); i++ ) {
        ValueMetaInterface vmi = rmi.getValueMeta( i );
        Schema.Field field = schema.getField( vmi.getName() );
        Object defaultVal = field.defaultVal();

        switch ( vmi.getType() ) {
          case ValueMetaInterface.TYPE_STRING:
            String defaultStringValue = String.valueOf( field.defaultVal() );
            outputRecord.put( vmi.getName(), row.getString( i, defaultStringValue ) );
            break;
          case ValueMetaInterface.TYPE_INTEGER:
            long defaultLong = -1;
            if ( defaultVal != null ) {
              if ( defaultVal instanceof String ) {
                defaultLong = Long.parseLong( (String) defaultVal );
              } else if ( defaultVal instanceof Long ) {
                defaultLong = (Long) defaultVal;
              }
              outputRecord.put( vmi.getName(), row.getInteger( i, defaultLong ) );
            } else {
              outputRecord.put( vmi.getName(), row.getInteger( i ) );
            }
            break;
          case ValueMetaInterface.TYPE_NUMBER:
            double defaultDouble = ( defaultVal == null
                || !( defaultVal instanceof Long ) ) ? 0 : (Double) defaultVal;
            outputRecord.put( vmi.getName(), row.getNumber( i, defaultDouble ) );
            break;
          case ValueMetaInterface.TYPE_BIGNUMBER:
            Double defaultBigDecimal = ( defaultVal == null
                || !( defaultVal instanceof Double ) ) ? null : ( (Double) defaultVal );
            outputRecord.put( vmi.getName(), row.getNumber( i, defaultBigDecimal ) );
            break;
          case ValueMetaInterface.TYPE_DATE:
            outputRecord.put( vmi.getName(), row.getInteger( i ) );
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            boolean defaultBoolean =
                defaultVal != null && ( !( defaultVal instanceof Boolean )
                    ? Boolean.parseBoolean( (String) defaultVal ) : (Boolean) defaultVal );
            outputRecord.put( vmi.getName(), row.getBoolean( i, defaultBoolean ) );
            break;
          case ValueMetaInterface.TYPE_BINARY:
            outputRecord.put( vmi.getName(), row.getBinary( i, ObjectToByteArray( defaultVal ) ) );
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

  public  byte[] ObjectToByteArray( Object obj )  {
    if ( obj == null ) {
      return null;
    }

    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ObjectOutputStream os = new ObjectOutputStream( out );
      os.writeObject( obj );
      return out.toByteArray();
    } catch ( IOException ioex ) {
      return null;
    }
  }
}
