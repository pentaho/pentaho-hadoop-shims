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
import org.junit.Test;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;


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
    assertEquals( "formatFieldName", fields.get( 0 ).name() );

    String fullName = avroSchema.getFullName();
    assertEquals( "ns.recordname", fullName );

    String name = avroSchema.getName();
    assertEquals( "recordname", name );
  }

}
