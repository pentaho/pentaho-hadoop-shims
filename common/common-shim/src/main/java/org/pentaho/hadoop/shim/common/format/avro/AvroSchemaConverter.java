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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

/**
 * Converts a SchemaDesciption to a Avro Schema
 * Created by tkafalas on 8/29/2017.
 */
public class AvroSchemaConverter {
  private SchemaDescription schemaDescription;
  private ObjectMapper mapper = new ObjectMapper();

  private final String AVRO_TYPE_STRING = "string";
  private final String AVRO_TYPE_DOUBLE = "double";
  private final String AVRO_TYPE_INTEGER = "int";
  private final String AVRO_TYPE_BOOLEAN = "boolean";
  private final String AVRO_TYPE_BINARY = "bytes";
  private final String AVRO_TYPE_DATE = "int";
  private final String AVRO_TYPE_TIMESTAMP = "long";
  private final String AVRO_TYPE_NULL = "null";
  private final String AVRO_TYPE_RECORD = "record";
  private final String AVRO_FIELDS_NODE = "fields";
  private final String AVRO_LOGICAL_TYPE = "logicalType";
  private final String AVRO_NAMESPACE_NODE = "namespace";
  private final String AVRO_NAME_NODE = "name";
  private final String AVRO_TYPE_NODE = "type";
  private final String AVRO_DEFAULT_NODE = "default";
  Schema schema;

  public AvroSchemaConverter( SchemaDescription schemaDescription ) {
    this.schemaDescription = schemaDescription;
  }

  public Schema createAvroSchema() {
    if ( schemaDescription == null ) {
      return null;
    }
    ObjectNode rootNode = mapper.createObjectNode();
    ArrayNode fieldNodes = mapper.createArrayNode();

    schemaDescription.forEach( f -> fieldNodes.add( convertField( f ) ) );
    rootNode.put( AVRO_NAMESPACE_NODE, "myNamespace" );
    rootNode.put( AVRO_TYPE_NODE, AVRO_TYPE_RECORD );
    rootNode.put( AVRO_NAME_NODE, "testRecord" );
    rootNode.putPOJO( AVRO_FIELDS_NODE, fieldNodes );
    Schema schema = new Schema.Parser().parse( rootNode.toString() );
    return schema;
  }

  private ObjectNode convertField( SchemaDescription.Field f ) {
    switch ( f.pentahoValueMetaType ) {
      case ValueMetaInterface.TYPE_NUMBER:
        return convertPrimitive( AVRO_TYPE_DOUBLE, f );
      case ValueMetaInterface.TYPE_STRING:
        return convertPrimitive( AVRO_TYPE_STRING, f );
      case ValueMetaInterface.TYPE_BOOLEAN:
        return convertPrimitive( AVRO_TYPE_BOOLEAN, f );
      case ValueMetaInterface.TYPE_INTEGER:
        return convertPrimitive( AVRO_TYPE_INTEGER, f );
      case ValueMetaInterface.TYPE_BIGNUMBER:
        return convertPrimitive( AVRO_TYPE_DOUBLE, f );
      case ValueMetaInterface.TYPE_SERIALIZABLE:
        return convertPrimitive( AVRO_TYPE_BINARY, f );
      case ValueMetaInterface.TYPE_BINARY:
        return convertPrimitive( AVRO_TYPE_BINARY, f );
      case ValueMetaInterface.TYPE_DATE:
        return convertPrimitive( AVRO_TYPE_DATE, f );
      case ValueMetaInterface.TYPE_TIMESTAMP:
        return convertPrimitive( AVRO_TYPE_TIMESTAMP, f );
      case ValueMetaInterface.TYPE_INET:
        return convertPrimitive( AVRO_TYPE_STRING, f );
      default:
        throw new RuntimeException( "Field: " + f.formatFieldName + "  Undefined type: " + f.pentahoValueMetaType );
    }
  }

  private ObjectNode convertPrimitive( String type, SchemaDescription.Field f ) {
    ObjectNode fieldNode = mapper.createObjectNode();

    fieldNode.put( AVRO_NAME_NODE, f.formatFieldName );
    if ( f.allowNull ) {
      fieldNode.putPOJO( AVRO_TYPE_NODE, mapper.createArrayNode().add( AVRO_TYPE_NULL ).add( type ) );
    } else {
      fieldNode.put( AVRO_TYPE_NODE, type );
    }
    if ( f.pentahoValueMetaType == ValueMetaInterface.TYPE_DATE ) {
      fieldNode.put( AVRO_LOGICAL_TYPE, "date" );
    } else if ( f.pentahoValueMetaType == ValueMetaInterface.TYPE_DATE ) {
      fieldNode.put( AVRO_LOGICAL_TYPE, "timestamp-micros" );
    }
    if ( f.defaultValue != null ) {
      fieldNode.put( AVRO_DEFAULT_NODE, f.defaultValue );
    }
    return fieldNode;
  }

}
