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

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.hadoop.shim.api.format.IPentahoAvroOutputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import java.util.zip.Deflater;

/**
 * @author tkafalas
 */
public class PentahoAvroOutputFormat implements IPentahoAvroOutputFormat {
  private Schema schema;
  private String outputFilename;
  private SchemaDescription schemaDescription;
  private CodecFactory codecFactory;

  private String nameSpace;
  private String recordName;
  private String docValue;
  private String schemaFilename;

  @Override
  public IPentahoRecordWriter createRecordWriter() throws Exception {
    if ( schemaDescription == null || StringUtils.isEmpty( nameSpace ) || StringUtils.isEmpty( recordName ) || StringUtils.isEmpty( outputFilename ) ) {
      throw new Exception( "Invalid state.  One of the following required fields is null:  'nameSpace', 'recordNum', or 'outputFileName" );
    }
    AvroSchemaConverter converter = new AvroSchemaConverter( schemaDescription, nameSpace, recordName, docValue );
    schema = converter.getAvroSchema();
    converter.writeAvroSchemaToFile( schemaFilename );
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>( schema );
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>( datumWriter );
    dataFileWriter.setCodec( codecFactory );
    dataFileWriter.create( schema, KettleVFS.getOutputStream( outputFilename, false ) );
    return new PentahoAvroRecordWriter( dataFileWriter, schema, schemaDescription );
  }

  @Override
  public void setSchemaDescription( SchemaDescription schemaDescription ) throws Exception {
    this.schemaDescription = schemaDescription;
  }

  @Override
  public void setOutputFile( String file ) throws Exception {
    this.outputFilename = file;
  }

  @Override
  public void setCompression( COMPRESSION compression ) {
    switch ( compression ) {
      case SNAPPY:
        codecFactory = CodecFactory.snappyCodec();
        break;
      case DEFLATE:
        codecFactory = CodecFactory.deflateCodec( Deflater.DEFAULT_COMPRESSION );
        break;
      default:
        codecFactory = CodecFactory.nullCodec();
        break;
    }
  }

  @Override
  public void setNameSpace( String namespace ) {
    this.nameSpace = namespace;
  }

  @Override
  public void setRecordName( String recordName ) {
    this.recordName = recordName;
  }

  @Override
  public void setDocValue( String docValue ) {
    this.docValue = docValue;
  }

  @Override
  public void setSchemaFilename( String schemaFilename ) {
    this.schemaFilename = schemaFilename;
  }

}
