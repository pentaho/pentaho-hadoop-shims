/*******************************************************************************
 *
 * Pentaho Big Data
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
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.pentaho.hadoop.shim.api.format.IPentahoAvroOutputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import java.io.File;

/**
 * 
 * @author tkafalas
 */
public class PentahoAvroOutputFormat implements IPentahoAvroOutputFormat {
  private Schema schema;
  private String file;
  private SchemaDescription schemaDescription;

  @Override
  public IPentahoRecordWriter createRecordWriter() throws Exception {
    schema = ( new AvroSchemaConverter( schemaDescription ) ).createAvroSchema();
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>( schema );
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>( datumWriter );
    File contentFile = new File( file );
    dataFileWriter.create( schema, contentFile );
    return new PentahoAvroRecordWriter( dataFileWriter, schema, null );
  }

  @Override
  public void setSchema( SchemaDescription schemaDescription ) throws Exception {
    this.schemaDescription = schemaDescription;
  }

  @Override
  public void setOutputFile( String file ) throws Exception {
    this.file = file;
 }

  @Override
  public void setCompression( COMPRESSION compression ) {
    // TODO Auto-generated method stub

  }

}
