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

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

/**
 * Converts a SchemaDesciption to a Avro Schema
 * Created by tkafalas on 8/29/2017.
 */
public class AvroSchemaConverter {

  private final String AVRO_TYPE_STRING = "string";
  private final String AVRO_TYPE_DOUBLE = "double";
  private final String AVRO_TYPE_LONG = "long";
  private final String AVRO_TYPE_BOOLEAN = "boolean";
  private final String AVRO_TYPE_BINARY = "bytes";
  private final String AVRO_TYPE_DATE = "int";
  private final String AVRO_TYPE_TIMESTAMP = "long";
  private final String AVRO_TYPE_NULL = "null";
  private final String AVRO_TYPE_RECORD = "record";
  private final String AVRO_DOC = "doc";
  private final String AVRO_FIELDS_NODE = "fields";
  private final String AVRO_LOGICAL_TYPE = "logicalType";
  private final String AVRO_NAMESPACE_NODE = "namespace";
  private final String AVRO_NAME_NODE = "name";
  private final String AVRO_TYPE_NODE = "type";
  private final String AVRO_DEFAULT_NODE = "default";

  private ObjectMapper mapper = new ObjectMapper();
  private final ObjectNode avroSchema;

  public AvroSchemaConverter( SchemaDescription schemaDescription, String nameSpace, String recordName, String docValue ) {
    if ( schemaDescription != null ) {
      avroSchema = mapper.createObjectNode();
      ArrayNode fieldNodes = mapper.createArrayNode();

      schemaDescription.forEach( f -> fieldNodes.add( convertField( f ) ) );
      avroSchema.put( AVRO_NAMESPACE_NODE, nameSpace );
      avroSchema.put( AVRO_TYPE_NODE, AVRO_TYPE_RECORD );
      avroSchema.put( AVRO_NAME_NODE, recordName );
      avroSchema.put( AVRO_DOC, docValue );
      avroSchema.putPOJO( AVRO_FIELDS_NODE, fieldNodes );
    } else {
      avroSchema = null;
    }
  }

  public void writeAvroSchemaToFile( String schemaFilename ) throws JsonGenerationException, JsonMappingException, KettleFileException, IOException {
    if ( avroSchema != null && schemaFilename != null ) {
      ObjectWriter writer = mapper.writer( new DefaultPrettyPrinter() );
      writer.writeValue( KettleVFS.getOutputStream( schemaFilename, false ), avroSchema );
    }
  }

  public Schema getAvroSchema() {
    return avroSchema == null ? null : new Schema.Parser().parse( avroSchema.toString() );
  }

  public static SchemaDescription createSchemaDescription( Schema schema ) {
    SchemaDescription sd = new SchemaDescription();
    schema.getFields().forEach( f -> sd.addField( convertField( sd, f ) ) );
    return sd;
  }

  @VisibleForTesting
  ObjectNode convertField( SchemaDescription.Field f ) {
    switch ( f.pentahoValueMetaType ) {
      case ValueMetaInterface.TYPE_NUMBER:
        return convertPrimitive( AVRO_TYPE_DOUBLE, f );
      case ValueMetaInterface.TYPE_STRING:
        return convertPrimitive( AVRO_TYPE_STRING, f );
      case ValueMetaInterface.TYPE_BOOLEAN:
        return convertPrimitive( AVRO_TYPE_BOOLEAN, f );
      case ValueMetaInterface.TYPE_INTEGER:
        return convertPrimitive( AVRO_TYPE_LONG, f );
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


  @VisibleForTesting
  private static SchemaDescription.Field convertField( SchemaDescription schema, Schema.Field f ) {
    boolean allowNull = f.defaultVal() == null;
    String defaultValue = null;
    if ( !allowNull ) {
      defaultValue = f.defaultVal().toString();
    }

    Schema.Type schemaType = null;
    if ( f.schema().getType().equals( Schema.Type.UNION ) ) {
      List<Schema> schemas = f.schema().getTypes();
      for ( Schema s: schemas ) {
        if ( !s.getName().equalsIgnoreCase( "null" ) ) {
          schemaType = s.getType();
          break;
        }
      }
    } else {
      schemaType = f.schema().getType();
    }

    switch ( schemaType ) {
      case DOUBLE:
      case FLOAT:
        return schema.new Field( f.name(), f.name(), ValueMetaInterface.TYPE_NUMBER, allowNull );
      case LONG:
        if ( f.schema().getLogicalType() != null ) {
          String logicalType = f.schema().getLogicalType().getName();
          if ( logicalType.equals( LogicalTypes.timeMillis() ) ) {
            return schema.new Field( f.name(), f.name(), ValueMetaInterface.TYPE_TIMESTAMP, defaultValue, allowNull );
          }
        } else {
          return schema.new Field( f.name(), f.name(), ValueMetaInterface.TYPE_INTEGER, defaultValue, allowNull );
        }
      case BOOLEAN:
        return schema.new Field( f.name(), f.name(), ValueMetaInterface.TYPE_BOOLEAN, defaultValue, allowNull );
      case BYTES:
        return schema.new Field( f.name(), f.name(), ValueMetaInterface.TYPE_STRING, defaultValue, allowNull );
      case INT:
        if ( f.schema().getLogicalType() != null ) {
          String logicalType = f.schema().getLogicalType().getName();
          if ( logicalType.equals( LogicalTypes.date() ) ) {
            return schema.new Field( f.name(), f.name(), ValueMetaInterface.TYPE_DATE, defaultValue, allowNull );
          }
        }
        break;
      case STRING:
        return schema.new Field( f.name(), f.name(), ValueMetaInterface.TYPE_STRING, defaultValue, allowNull );
      default:
        throw new RuntimeException( "Field: " + f.name() + "  Undefined type: " + f.schema().getType() );
    }
    return null;
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
    } else if ( f.pentahoValueMetaType == ValueMetaInterface.TYPE_TIMESTAMP ) {
      //we able to keep only timestamp-millis because we are using the old java date format which does not support microsecond
      fieldNode.put( AVRO_LOGICAL_TYPE, "timestamp-millis" );
    }
    if ( f.defaultValue != null ) {
      fieldNode.put( AVRO_DEFAULT_NODE, f.defaultValue );
    }
    return fieldNode;
  }
}
