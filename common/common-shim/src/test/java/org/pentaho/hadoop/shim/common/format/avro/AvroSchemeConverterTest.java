/*! ******************************************************************************
 *
 * Pentaho Data Integration
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
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import static org.junit.Assert.*;

/**
 * Created by tkafalas on 8/30/2017.
 */
public class AvroSchemeConverterTest {
  private int AVRO_FIELD_NAME = 0;
  private int PENTAHO_FIELD_NAME = 1;
  private int PENTAHO_FIELD_TYPE = 2;
  private int ALLOW_NULL = 3;
  private int DEFAULT_VALUE = 4;

  String[][] fieldData = new String[][] {
    { "AvroString1", "PentahoString", String.valueOf( ValueMetaInterface.TYPE_STRING ), "false", "defaultString" },
    { "AvroInt", "PentahoInt", String.valueOf( ValueMetaInterface.TYPE_INTEGER ), "true", null },
    { "AvroBoolean", "PentahoBoolean", String.valueOf( ValueMetaInterface.TYPE_BOOLEAN ), "true", null },
    { "AvroDouble1", "PentahoNumber", String.valueOf( ValueMetaInterface.TYPE_NUMBER ), "true", "10.1" },
    { "AvroDouble2", "PentahoBigNumber", String.valueOf( ValueMetaInterface.TYPE_BIGNUMBER ), "false", "123.0" },
    { "AvroString2", "PentahoSerializable", String.valueOf( ValueMetaInterface.TYPE_SERIALIZABLE ), "true", "ssssss" },
    { "AvroBytes", "PentahoBinary", String.valueOf( ValueMetaInterface.TYPE_BINARY ), "true", "bbbbb" },
    { "AvroDate", "PentahoDate", String.valueOf( ValueMetaInterface.TYPE_DATE ), "true", "100" },
    { "AvroTimestamp", "PentahoTimestamp", String.valueOf( ValueMetaInterface.TYPE_INET ), "true", "100" },
    { "AvroString3", "PentahoInetAddress", String.valueOf( ValueMetaInterface.TYPE_TIMESTAMP ), "true", "110" }
  };

  SchemaDescription schemaDescription;
  AvroSchemaConverter avroSchemaConverter;

  @Before
  public void setUp() {
    schemaDescription = new SchemaDescription();
    for ( int i = 0; i < fieldData.length; i++ ) {
      SchemaDescription.Field field =
        schemaDescription.new Field( fieldData[ i ][ AVRO_FIELD_NAME ], fieldData[ i ][ PENTAHO_FIELD_NAME ],
          Integer.valueOf( fieldData[ i ][ PENTAHO_FIELD_TYPE ] ), "true".equals( fieldData[ i ][ ALLOW_NULL ] ) );
      field.defaultValue = fieldData[ i ][ DEFAULT_VALUE ];
      schemaDescription.addField( field );
    }
    avroSchemaConverter = new AvroSchemaConverter( schemaDescription, "sampleName", "sampleRecord", "sampleDoc" );
  }

  @Test
  public void testCreateAvroSchema() {
    Schema schema = avroSchemaConverter.getAvroSchema();
    for ( int i = 0; i < fieldData.length; i++ ) {
      Schema.Field f = schema.getField( fieldData[ i ][ AVRO_FIELD_NAME ] );

      //Check default values
      if ( convertDefault( i ) == null ) {
        assertEquals( convertDefault( i ), f.defaultVal() );
      } else if ( "true".equals( fieldData[ i ][ ALLOW_NULL ] ) ) {
        assertEquals( convertDefault( i ).toString(), f.defaultVal().toString() );
      } else {
        assertEquals( convertDefault( i ), f.defaultVal() );
      }
    }
  }

  private Object convertDefault( int fieldIndex ) {
    int valueMetaType = Integer.valueOf( fieldData[ fieldIndex ][ PENTAHO_FIELD_TYPE ] );

    if ( fieldData[ fieldIndex ][ DEFAULT_VALUE ] != null ) {
      switch ( valueMetaType ) {
        case ValueMetaInterface.TYPE_NUMBER:
          return Double.valueOf( fieldData[ fieldIndex ][ DEFAULT_VALUE ] );
        case ValueMetaInterface.TYPE_STRING:
          return fieldData[ fieldIndex ][ DEFAULT_VALUE ];
        case ValueMetaInterface.TYPE_BOOLEAN:
          return Boolean.valueOf( fieldData[ fieldIndex ][ DEFAULT_VALUE ] );
        case ValueMetaInterface.TYPE_INTEGER:
          return Integer.valueOf( fieldData[ fieldIndex ][ DEFAULT_VALUE ] );
        case ValueMetaInterface.TYPE_BIGNUMBER:
          return Double.valueOf( fieldData[ fieldIndex ][ DEFAULT_VALUE ] );
        case ValueMetaInterface.TYPE_SERIALIZABLE:
          return fieldData[ fieldIndex ][ DEFAULT_VALUE ];
        case ValueMetaInterface.TYPE_BINARY:
          return fieldData[ fieldIndex ][ DEFAULT_VALUE ];
        case ValueMetaInterface.TYPE_DATE:
          return Integer.valueOf( fieldData[ fieldIndex ][ DEFAULT_VALUE ] );
        case ValueMetaInterface.TYPE_TIMESTAMP:
          return Long.valueOf( fieldData[ fieldIndex ][ DEFAULT_VALUE ] );
        case ValueMetaInterface.TYPE_INET:
          return fieldData[ fieldIndex ][ DEFAULT_VALUE ];
      }
    }
    return null;
  }
}
