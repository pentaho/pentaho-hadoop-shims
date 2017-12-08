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
package org.pentaho.hadoop.shim.common.format.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import java.io.IOException;

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
    try {
      nativeAvroRecordWriter.append( AvroConverter.convertToAvro( row, schema, schemaDescription ) );
    } catch ( IOException e ) {

    }
  }

  @Override
  public void close() throws IOException {
    nativeAvroRecordWriter.close();
  }
}
