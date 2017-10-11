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

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith( Parameterized.class )
public class AvroSchemaConverterFieldsTest {

  private String result;
  private int type;

  public AvroSchemaConverterFieldsTest( int type, String result ) {
    this.type = type;
    this.result = result;
  }

  @Parameterized.Parameters
  public static Collection values() {
    return Arrays.asList( new Object[][] {
      { ValueMetaInterface.TYPE_NUMBER, "double" },
      { ValueMetaInterface.TYPE_STRING, "string" },
      { ValueMetaInterface.TYPE_BOOLEAN, "boolean" },
      { ValueMetaInterface.TYPE_INTEGER, "long" },
      { ValueMetaInterface.TYPE_BIGNUMBER, "double" },
      { ValueMetaInterface.TYPE_SERIALIZABLE, "bytes" },
      { ValueMetaInterface.TYPE_BINARY, "bytes" },
      { ValueMetaInterface.TYPE_DATE, "int" },
      { ValueMetaInterface.TYPE_TIMESTAMP, "long" },
      { ValueMetaInterface.TYPE_INET, "string" }
    } );
  }

  @Test
  public void testConvertFields() throws Exception {
    SchemaDescription description = new SchemaDescription();
    AvroSchemaConverter converter = new AvroSchemaConverter( description, "ns", "recordname", "docValue" );
    SchemaDescription.Field f = createField( description, type );
    ObjectNode jsonNodes = converter.convertField( f );
    assertEquals( result, jsonNodes.findValuesAsText( "type" ).get( 0 ) );
  }

  private SchemaDescription.Field createField( SchemaDescription description, int type ) {
    return description.new Field( "ffn", "pfn", type, false );
  }

}
