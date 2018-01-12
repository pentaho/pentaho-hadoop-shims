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

import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.vfs2.FileExtensionSelector;
import org.apache.commons.vfs2.FileObject;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.hadoop.shim.api.format.AvroSpec;
import org.pentaho.hadoop.shim.api.format.IAvroInputField;
import org.pentaho.hadoop.shim.api.format.IPentahoAvroInputFormat;

public class PentahoAvroInputFormat implements IPentahoAvroInputFormat {

  private String fileName;
  private String schemaFileName;
  private List<? extends IAvroInputField> inputFields;

  @Override
  public List<IPentahoInputSplit> getSplits() throws Exception {
    return null;
  }

  @Override
    public IPentahoRecordReader createRecordReader( IPentahoInputSplit split ) throws Exception {
    DataFileStream<GenericRecord> dfs = createDataFileStream(  );
    if ( dfs == null ) {
      throw new Exception( "Unable to read data from file " + fileName );
    }
    Schema avroSchema = readAvroSchema( );

    return new PentahoAvroRecordReader( dfs, avroSchema, getFields() );
  }

  @VisibleForTesting
  public Schema readAvroSchema( ) throws Exception {
    if ( schemaFileName != null && schemaFileName.length() > 0 ) {
      return new Schema.Parser().parse( KettleVFS.getInputStream( schemaFileName ) );
   } else if ( fileName != null && fileName.length() > 0 ) {
      Schema schema = null;
      DataFileStream<GenericRecord> dataFileStream = createDataFileStream(  );
      schema = dataFileStream.getSchema();
      dataFileStream.close();
      return  schema;
    }
    throw new Exception( "The file you provided does not contain a schema."
          + "  Please choose a schema file, or another file that contains a schema." );
  }

  @Override
  public List<? extends IAvroInputField> getFields() throws Exception {
    if (this.inputFields != null) {
      return inputFields;
    } else {
      return getDefaultFields();
    }
  }

  @Override
  public void setInputFields( List<? extends IAvroInputField> fields ) throws Exception {
    this.inputFields = fields;
  }

  @Override
  public void setInputFile( String fileName ) throws Exception {
    this.fileName = fileName;
  }

  @Override
  public void setInputSchemaFile( String schemaFileName ) throws Exception {
    this.schemaFileName = schemaFileName;
  }


  @Override
  public void setSplitSize( long blockSize ) throws Exception {
    //do nothing 
  }

  private DataFileStream<GenericRecord> createDataFileStream(  ) throws Exception {
    DatumReader<GenericRecord> datumReader;
    if ( schemaFileName != null && schemaFileName.length() > 0 ) {
      Schema schema = new Schema.Parser().parse( KettleVFS.getInputStream( schemaFileName ) );
      datumReader = new GenericDatumReader<GenericRecord>( schema );
    } else {
      datumReader = new GenericDatumReader<GenericRecord>(  );
    }
    FileObject fileObject = KettleVFS.getFileObject( fileName );
    if ( fileObject.isFile() ) {
      return  new DataFileStream<GenericRecord>( fileObject.getContent().getInputStream(), datumReader );
    } else {
      FileObject[] avroFiles = fileObject.findFiles( new FileExtensionSelector( "avro" ) );
      if ( !Utils.isEmpty( avroFiles ) ) {
        return  new DataFileStream<GenericRecord>( avroFiles[0].getContent().getInputStream(), datumReader );
      }
      return null;
    }
  }

  public List<? extends IAvroInputField> getDefaultFields( ) throws Exception {
    ArrayList<AvroInputField> fields = new ArrayList<AvroInputField>();

    Schema avroSchema = readAvroSchema();
    for (Schema.Field f : avroSchema.getFields()) {

      String logicalType = f.getProp( AvroSpec.LOGICAL_TYPE );
      AvroSpec.DataType actualAvroType = null;
      if (logicalType != null) {
        for (AvroSpec.DataType tmpType : AvroSpec.DataType.values()) {
          if (logicalType.equals( tmpType.getLogicalType() )) {
            actualAvroType = tmpType;
            break;
          }
        }
      } else {
        String primitiveType = null;
        if ( f.schema().getType().equals( Schema.Type.UNION ) ) {
          List<Schema> schemas = f.schema().getTypes();
          for ( Schema s: schemas ) {
            if ( !s.getName().equalsIgnoreCase( "null" ) ) {
              primitiveType = s.getType().getName();
              break;
            }
          }
        } else {
          primitiveType = f.schema().getType().getName();
        }

        for (AvroSpec.DataType tmpType : AvroSpec.DataType.values()) {
          if (primitiveType.equals( tmpType.getBaseType() )) {
            actualAvroType = tmpType;
            break;
          }
        }
      }

      AvroSpec.DataType supportedAvroType = null;
      if ((actualAvroType == AvroSpec.DataType.DATE)
        || (actualAvroType == AvroSpec.DataType.DECIMAL)
        || (actualAvroType == AvroSpec.DataType.TIME_MILLIS)
        || actualAvroType.isPrimitiveType()) {
        supportedAvroType = actualAvroType;
      }

      if (supportedAvroType == null) {
        throw new RuntimeException( "Field: " + f.name() + "  Undefined type: " + f.schema().getType() );
      }

      int pentahoType = 0;
      switch ( supportedAvroType ) {
        case DATE:
          pentahoType = ValueMetaInterface.TYPE_DATE;
          break;
        case DOUBLE:
          pentahoType = ValueMetaInterface.TYPE_NUMBER;
          break;
        case FLOAT:
          pentahoType = ValueMetaInterface.TYPE_NUMBER;
          break;
        case LONG:
          pentahoType = ValueMetaInterface.TYPE_INTEGER;
          break;
        case BOOLEAN:
          pentahoType = ValueMetaInterface.TYPE_BOOLEAN;
          break;
        case INTEGER:
          pentahoType = ValueMetaInterface.TYPE_INTEGER;
          break;
        case STRING:
          pentahoType = ValueMetaInterface.TYPE_STRING;
          break;
        case BYTES:
          pentahoType = ValueMetaInterface.TYPE_BINARY;
          break;
        case DECIMAL:
          pentahoType = ValueMetaInterface.TYPE_BIGNUMBER;
          break;
        case TIME_MILLIS:
          pentahoType = ValueMetaInterface.TYPE_TIMESTAMP;
          break;
      }

      AvroInputField avroInputField = new AvroInputField();
      avroInputField.setAvroFieldName(f.name());
      avroInputField.setPentahoFieldName(f.name());
      avroInputField.setPentahoType(pentahoType);
      avroInputField.setAvroType( actualAvroType );
      fields.add( avroInputField );
    }

    return fields;
  }
}
