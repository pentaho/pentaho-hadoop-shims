/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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
import org.junit.Test;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class AvroSchemaConverterTest {

  @Test
  public void shouldReturnNullWhenSchemaDescriptionIsNull() throws Exception {
    AvroSchemaConverter converter = new AvroSchemaConverter( null, null, null, null );
    assertNull( converter.getAvroSchema() );
  }

  @Test
  public void shoulReturnSchema() throws Exception {
    SchemaDescription description = new SchemaDescription();
    description.addField( description.new Field( "formatFieldName", "pentahoFieldName", ValueMetaInterface.TYPE_STRING, true ) );
    AvroSchemaConverter converter = new AvroSchemaConverter( description, "ns", "recordname", "docValue" );

    Schema avroSchema = converter.getAvroSchema();

    assertNotNull( avroSchema );
    assertEquals( "docValue", avroSchema.getDoc() );

    List<Schema.Field> fields = avroSchema.getFields();
    assertEquals( 1, fields.size() );
    assertEquals( "formatFieldName_delimiter_2_delimiter_true", fields.get( 0 ).name() );

    String fullName = avroSchema.getFullName();
    assertEquals( "ns.recordname", fullName );

    String name = avroSchema.getName();
    assertEquals( "recordname", name );
  }

  @Test
  public void shouldCreateSchemaDescription() throws Exception {
    Schema schema = mock( Schema.class );
    when( schema.getType() ).thenReturn( Schema.Type.BOOLEAN );

    Schema.Field f = mock( Schema.Field.class );
    when( f.schema() ).thenReturn( schema );
    when( f.name() ).thenReturn( "fieldName" );

    when( schema.getFields() ).thenReturn( Arrays.asList( f ) );
    SchemaDescription schemaDescription = AvroSchemaConverter.createSchemaDescription( schema );

    List<SchemaDescription.Field> fields = StreamSupport
      .stream( schemaDescription.spliterator(), false ).collect( Collectors.toList() );
    assertEquals( 1, fields.size() );
  }
}
