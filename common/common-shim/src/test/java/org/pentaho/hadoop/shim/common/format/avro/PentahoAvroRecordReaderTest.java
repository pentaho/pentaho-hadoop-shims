/*! ******************************************************************************
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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import java.util.Iterator;

import static org.mockito.Mockito.verify;

@RunWith( MockitoJUnitRunner.class )
public class PentahoAvroRecordReaderTest {


  private @Mock DataFileStream<GenericRecord> nativeAvroRecordReader;
  private PentahoAvroRecordReader recordReader;

  @Before
  public void setUp() throws Exception {
    SchemaDescription avroSchemaDescription = new SchemaDescription();
    SchemaDescription metaSchemaDescription = new SchemaDescription();
    recordReader = new PentahoAvroRecordReader( nativeAvroRecordReader, avroSchemaDescription, metaSchemaDescription );
  }

  @Test
  public void shouldCloseNativeReader() throws Exception {
    recordReader.close();
    verify( nativeAvroRecordReader ).close();
  }

  @Test
  public void shouldCreateIterator() throws Exception {
    Iterator<RowMetaAndData> iterator = recordReader.iterator();

    iterator.hasNext();
    verify( nativeAvroRecordReader ).hasNext();

    iterator.next();
    verify( nativeAvroRecordReader ).next();
  }
}
