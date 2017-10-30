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

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.hadoop.shim.api.format.IPentahoAvroInputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by rmansoor on 10/5/2017.
 */
public class PentahoAvroRecordReader implements IPentahoAvroInputFormat.IPentahoRecordReader {

  private final DataFileStream<GenericRecord> nativeAvroRecordReader;
  private final SchemaDescription avroSchemaDescription;
  private final SchemaDescription metaSchemaDescription;

  public PentahoAvroRecordReader( DataFileStream<GenericRecord> nativeAvroRecordReader,
      SchemaDescription avroSchemaDescription, SchemaDescription metaSchemaDescription ) {
    this.nativeAvroRecordReader = nativeAvroRecordReader;
    this.avroSchemaDescription = avroSchemaDescription;
    this.metaSchemaDescription = metaSchemaDescription;
  }

  @Override public void close() throws IOException {
    nativeAvroRecordReader.close();
  }

  @Override public Iterator<RowMetaAndData> iterator() {
    return new Iterator<RowMetaAndData>() {

      @Override public boolean hasNext() {
        return nativeAvroRecordReader.hasNext();
      }

      @Override public RowMetaAndData next() {
        return AvroConverter.convertFromAvro(  nativeAvroRecordReader.next(), avroSchemaDescription, metaSchemaDescription );
      }
    };
  }
}
