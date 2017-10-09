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

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.log4j.Logger;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.hadoop.shim.api.format.IPentahoAvroInputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

public class PentahoAvroInputFormat implements IPentahoAvroInputFormat {

  private static final Logger logger = Logger.getLogger( PentahoAvroInputFormat.class );
  private String file;
  private String schemaFile;
  private SchemaDescription schema;
  private long splitSize;

  @Override
  public List<IPentahoInputSplit> getSplits() throws Exception {
    return null;
  }

  @Override
    public IPentahoRecordReader createRecordReader( IPentahoInputSplit split ) throws Exception {
    return new PentahoAvroRecordReader( createDataFileStream( schemaFile, file ), readSchema( schemaFile, file ) );
  }

  @Override
  public SchemaDescription readSchema( String schemaFile, String file ) throws Exception {
    if ( schemaFile != null && schemaFile.length() > 0 ) {
      return AvroSchemaConverter.createSchemaDescription( readAvroSchema( schemaFile ) );
    } else if ( file != null && file.length() > 0 ) {
        DataFileStream<GenericRecord> dataFileStream = createDataFileStream( schemaFile, file );
        SchemaDescription schemaDescription = AvroSchemaConverter.createSchemaDescription( dataFileStream.getSchema() );
        dataFileStream.close();
        return  schemaDescription;
    } else {
      throw new Exception( "Data file and schema file cannot be null" );
    }
  }

  private Schema readAvroSchema( String file ) throws KettleFileException, IOException{
      return new Schema.Parser().parse( KettleVFS.getInputStream( file ) );
  }

  @Override
  public void setSchema( SchemaDescription schema ) throws Exception {
    this.schema = schema;
  }

  @Override
  public void setInputFile( String file ) throws Exception {
    this.file = file;
  }

  @Override
  public void setInputSchemaFile( String schemaFile ) throws Exception {
    this.schemaFile = schemaFile;
  }


  @Override
  public void setSplitSize( long blockSize ) throws Exception {
    this.splitSize = blockSize;
  }

  private DataFileStream<GenericRecord> createDataFileStream( String schemaFile, String file ) throws Exception {
    DatumReader<GenericRecord> datumReader;
    if ( schemaFile != null && schemaFile.length() > 0 ) {
      datumReader = new GenericDatumReader<GenericRecord>( readAvroSchema( schemaFile ) );
    } else {
      datumReader = new GenericDatumReader<GenericRecord>(  );
    }
    return  new DataFileStream<GenericRecord>( KettleVFS.getInputStream( file ), datumReader );
  }
}
