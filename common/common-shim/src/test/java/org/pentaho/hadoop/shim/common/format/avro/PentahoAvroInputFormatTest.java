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

import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PentahoAvroInputFormatTest {

  public static final String SAMPLE_SCHEMA_AVRO = "/sample-schema.avro";
  public static final String SAMPLE_DATA_AVRO = "/sample-data.avro";

  private PentahoAvroInputFormat format;

  @Before
  public void setUp() throws Exception {
    format = new PentahoAvroInputFormat();
  }

  @Test
  public void getSplits() throws Exception {
    assertNull( format.getSplits() );
  }

  @Test( expected = Exception.class )
  public void readSchemaNoFiles() throws Exception {
    format.setInputFile( null );
    format.setInputSchemaFile( null );
    format.setInputFields( null );

    IPentahoInputFormat.IPentahoRecordReader reader = format.createRecordReader( null );
  }

  @Test
  public void readSchema() throws Exception {
    String schemaFile = getFilePath( SAMPLE_SCHEMA_AVRO );
    format.setInputSchemaFile( schemaFile );
    format.setInputFile( null );
    Schema   schema = format.readAvroSchema( );
    assertEquals( 2, schema.getFields().size() );
  }

  @Test
  public void readSchemaFromDataFile() throws Exception {
    String dataFile = getFilePath( SAMPLE_DATA_AVRO );
    format.setInputSchemaFile( null );
    format.setInputFile( dataFile );
    Schema   schema = format.readAvroSchema( );
    assertEquals( 2, schema.getFields().size() );
  }

  private String getFilePath( String file ) {
    return getClass().getResource( file ).getPath();
  }
}
