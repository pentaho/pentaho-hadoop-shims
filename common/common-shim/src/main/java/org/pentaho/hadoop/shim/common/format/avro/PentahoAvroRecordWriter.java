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

import java.io.IOException;

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
        switch ( vmi.getType() ) {
          case ValueMetaInterface.TYPE_STRING:
            outputRecord.put( vmi.getName(), row.getString( i, null ) );
            break;
          case ValueMetaInterface.TYPE_INTEGER:
            outputRecord.put( vmi.getName(), row.getInteger( i ) );
            break;
          case ValueMetaInterface.TYPE_NUMBER:
            outputRecord.put( vmi.getName(), row.getNumber( i, 0 ) );
            break;
          case ValueMetaInterface.TYPE_BIGNUMBER:
            outputRecord.put( vmi.getName(), row.getBigNumber( i, null ) );
            break;
          case ValueMetaInterface.TYPE_DATE:
            outputRecord.put( vmi.getName(), row.getInteger( i ) );
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            outputRecord.put( vmi.getName(), row.getBoolean( i, false ) );
            break;
          case ValueMetaInterface.TYPE_BINARY:
            outputRecord.put( vmi.getName(), row.getBinary( i, null ) );
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
