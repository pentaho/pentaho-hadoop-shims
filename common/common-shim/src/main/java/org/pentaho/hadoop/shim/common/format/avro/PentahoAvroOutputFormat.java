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

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.hadoop.shim.api.format.AvroSpec;
import org.pentaho.hadoop.shim.api.format.IAvroOutputField;
import org.pentaho.hadoop.shim.api.format.IPentahoAvroOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.zip.Deflater;

/**
 * @author tkafalas
 */
public class PentahoAvroOutputFormat implements IPentahoAvroOutputFormat {
  private String outputFilename;
  private List<? extends IAvroOutputField> fields;
  private CodecFactory codecFactory;

  private String nameSpace;
  private String recordName;
  private String docValue;
  private String schemaFilename;
  private Schema schema = null;
  ObjectNode schemaObjectNode = null;

  @Override
  public IPentahoRecordWriter createRecordWriter() throws Exception {
    if ( fields == null || StringUtils.isEmpty( nameSpace ) || StringUtils.isEmpty( recordName ) || StringUtils.isEmpty( outputFilename ) ) {
      throw new Exception( "Invalid state.  One of the following required fields is null:  'nameSpace', 'recordNum', or 'outputFileName" );
    }
    Schema schema = getSchema();
    writeAvroSchemaToFile( schemaFilename );
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>( schema );
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>( datumWriter );
    dataFileWriter.setCodec( codecFactory );
    dataFileWriter.create( schema, KettleVFS.getOutputStream( outputFilename, false ) );
    return new PentahoAvroRecordWriter( dataFileWriter, schema, fields );
  }

  @Override
  public void setFields( List<? extends IAvroOutputField> fields ) throws Exception {
    this.fields = fields;
    schema = null;
    schemaObjectNode = null;
  }

  @Override
  public void setOutputFile( String file ) throws Exception {
    this.outputFilename = file;
  }

  @Override
  public void setCompression( COMPRESSION compression ) {
    switch ( compression ) {
      case SNAPPY:
        codecFactory = CodecFactory.snappyCodec();
        break;
      case DEFLATE:
        codecFactory = CodecFactory.deflateCodec( Deflater.DEFAULT_COMPRESSION );
        break;
      default:
        codecFactory = CodecFactory.nullCodec();
        break;
    }
  }

  @Override
  public void setNameSpace( String namespace ) {
    this.nameSpace = namespace;
    schema = null;
    schemaObjectNode = null;
  }

  @Override
  public void setRecordName( String recordName ) {
    this.recordName = recordName;
    schema = null;
    schemaObjectNode = null;
  }

  @Override
  public void setDocValue( String docValue ) {
    this.docValue = docValue;
    schema = null;
    schemaObjectNode = null;
  }

  @Override
  public void setSchemaFilename( String schemaFilename ) {
    this.schemaFilename = schemaFilename;
  }

  protected Schema getSchema() {
    if ( schema == null ) {
        ObjectNode schemaObjectNode = getSchemaObjectNode();
        if ( schemaObjectNode != null ) {
          schema = new Schema.Parser().parse( schemaObjectNode.toString() );
        }
    }
    return schema;
  }

  protected ObjectNode getSchemaObjectNode() {
    if ( schemaObjectNode == null ) {
      if ( fields != null ) {
        ObjectMapper mapper = new ObjectMapper();
        schemaObjectNode = mapper.createObjectNode();

        schemaObjectNode.put( AvroSpec.NAMESPACE_NODE, nameSpace );
        schemaObjectNode.put( AvroSpec.TYPE_NODE, AvroSpec.TYPE_RECORD );
        schemaObjectNode.put( AvroSpec.NAME_NODE, recordName );
        schemaObjectNode.put( AvroSpec.DOC, docValue );

        ArrayNode fieldNodes = mapper.createArrayNode();
        Iterator<? extends IAvroOutputField> fields = this.fields.iterator();
        while ( fields.hasNext()) {
          IAvroOutputField f = fields.next();
          if ( f.getAvroType() == null ) {
            throw new RuntimeException( "Field: " + f.getAvroFieldName() + " has undefined type. " );
          }

          AvroSpec.DataType type = f.getAvroType();
          ObjectNode fieldNode = mapper.createObjectNode();

          fieldNode.put( AvroSpec.NAME_NODE, f.getAvroFieldName() );
          if ( type.isPrimitiveType()) {
            if ( f.getAllowNull() ) {
              ArrayNode arrayNode = mapper.createArrayNode().add( AvroSpec.DataType.NULL.getType() );
              arrayNode.add( type.getType() );
              fieldNode.putPOJO( AvroSpec.TYPE_NODE, arrayNode );
            } else {
              fieldNode.put( AvroSpec.TYPE_NODE, type.getType() );
            }
          } else {
            fieldNode.put( AvroSpec.LOGICAL_TYPE, type.getLogicalType() );
            if ( AvroSpec.DataType.DECIMAL == type) {
              fieldNode.put( AvroSpec.DECIMAL_PRECISION, 16);
              fieldNode.put( AvroSpec.DECIMAL_SCALE, 15);
            }
            if ( f.getAllowNull() ) {
              ArrayNode arrayNode = mapper.createArrayNode().add( AvroSpec.DataType.NULL.getType() );
              arrayNode.add( type.getBaseType() );
              fieldNode.putPOJO( AvroSpec.TYPE_NODE, arrayNode );
            } else {
              fieldNode.put( AvroSpec.TYPE_NODE, type.getBaseType() );
            }
          }
          if ( f.getDefaultValue() != null ) {
            fieldNode.put( AvroSpec.DEFAULT_NODE, f.getDefaultValue() );
          }
          fieldNodes.add( fieldNode );
        }
        schemaObjectNode.putPOJO( AvroSpec.FIELDS_NODE, fieldNodes );
      }
    }
    return schemaObjectNode;
  }

  protected void writeAvroSchemaToFile( String schemaFilename ) throws KettleFileException, IOException {
    ObjectNode schemaObjectNode = this.getSchemaObjectNode();
    if ( schemaObjectNode != null && schemaFilename != null ) {
      ObjectMapper mapper = new ObjectMapper();
      ObjectWriter writer = mapper.writer( new DefaultPrettyPrinter() );
      writer.writeValue( KettleVFS.getOutputStream( schemaFilename, false ), schemaObjectNode );
    }
  }

}
