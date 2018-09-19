/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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

import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.vfs2.FileExtensionSelector;
import org.apache.commons.vfs2.FileObject;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.hadoop.shim.api.format.AvroSpec;
import org.pentaho.hadoop.shim.api.format.IAvroInputField;
import org.pentaho.hadoop.shim.api.format.IPentahoAvroInputFormat;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class PentahoAvroInputFormat implements IPentahoAvroInputFormat {

  private String fileName;
  private String schemaFileName;
  private List<? extends IAvroInputField> inputFields;
  private String inputStreamFieldName;
  private boolean useFieldAsInputStream;
  private InputStream inputStream;
  private VariableSpace variableSpace;
  private Object[] incomingFields; //********* get the incoming fields to the step and delete this assignment

  private RowMetaInterface outputRowMeta;

  @Override
  public List<IPentahoInputSplit> getSplits() throws Exception {
    return null;
  }

  @Override
    public IPentahoRecordReader createRecordReader( IPentahoInputSplit split ) throws Exception {

    DataFileStream<Object> nestedDfs = createNestedDataFileStream();
      if ( nestedDfs == null ) {
        throw new Exception( "Unable to read data from file " + fileName );
      }
    Schema avroSchema = readAvroSchema();
      return new AvroNestedRecordReader( nestedDfs, avroSchema, getFields(), variableSpace, incomingFields,
        outputRowMeta );

  }

  @VisibleForTesting
  public Schema readAvroSchema( ) throws Exception {
    if ( schemaFileName != null && schemaFileName.length() > 0 ) {
      return new Schema.Parser().parse( KettleVFS.getInputStream( schemaFileName ) );
    } else if ( ( fileName != null && fileName.length() > 0 ) || ( useFieldAsInputStream && inputStream != null ) ) {
      Schema schema;
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
    if ( this.inputFields != null ) {
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
  public String getInputStreamFieldName() {
    return inputStreamFieldName;
  }

  @Override
  public void setInputStreamFieldName( String inputStreamFieldName ) {
    this.inputStreamFieldName = inputStreamFieldName;
    this.useFieldAsInputStream = inputStreamFieldName != null && !inputStreamFieldName.isEmpty();
  }

  @Override
  public boolean isUseFieldAsInputStream() {
    return useFieldAsInputStream;
  }

  @Override
  public void setInputStream( InputStream inputStream ) {
    this.inputStream = inputStream;
  }


  @Override
  public void setSplitSize( long blockSize ) throws Exception {
    //do nothing 
  }

  private DataFileStream<GenericRecord> createDataFileStream(  ) throws Exception {
    DatumReader<GenericRecord> datumReader;
    if ( useFieldAsInputStream ) {
      datumReader = new GenericDatumReader<GenericRecord>(  );
      inputStream.reset();
      return new DataFileStream<GenericRecord>( inputStream, datumReader );
    }
    if ( schemaFileName != null && schemaFileName.length() > 0 ) {
      Schema schema = new Schema.Parser().parse( KettleVFS.getInputStream( schemaFileName ) );
      datumReader = new GenericDatumReader<GenericRecord>( schema );
    } else {
      datumReader = new GenericDatumReader<GenericRecord>(  );
    }
    FileObject fileObject = KettleVFS.getFileObject( fileName );
    if ( fileObject.isFile() ) {
      this.inputStream = fileObject.getContent().getInputStream();
      return  new DataFileStream<>( inputStream, datumReader );
    } else {
      FileObject[] avroFiles = fileObject.findFiles( new FileExtensionSelector( "avro" ) );
      if ( !Utils.isEmpty( avroFiles ) ) {
        this.inputStream = avroFiles[0].getContent().getInputStream();
        return  new DataFileStream<>( inputStream, datumReader );
      }
      return null;
    }
  }

  private DataFileStream<Object> createNestedDataFileStream(  ) throws Exception {
    DatumReader<Object> datumReader;
    if ( useFieldAsInputStream ) {
      datumReader = new GenericDatumReader<Object>(  );
      inputStream.reset();
      return new DataFileStream<Object>( inputStream, datumReader );
    }
    if ( schemaFileName != null && schemaFileName.length() > 0 ) {
      Schema schema = new Schema.Parser().parse( KettleVFS.getInputStream( schemaFileName ) );
      datumReader = new GenericDatumReader<Object>( schema );
    } else {
      datumReader = new GenericDatumReader<Object>(  );
    }
    FileObject fileObject = KettleVFS.getFileObject( fileName );
    if ( fileObject.isFile() ) {
      this.inputStream = fileObject.getContent().getInputStream();
      return  new DataFileStream<>( inputStream, datumReader );
    } else {
      FileObject[] avroFiles = fileObject.findFiles( new FileExtensionSelector( "avro" ) );
      if ( !Utils.isEmpty( avroFiles ) ) {
        this.inputStream = avroFiles[0].getContent().getInputStream();
        return  new DataFileStream<>( inputStream, datumReader );
      }
      return null;
    }
  }

  public List<? extends IAvroInputField> getDefaultFields( ) throws Exception {
    ArrayList<AvroInputField> fields = new ArrayList<>();

    Schema avroSchema = readAvroSchema();
    for ( Schema.Field f : avroSchema.getFields() ) {
      AvroSpec.DataType actualAvroType = findActualDataType( f );
      AvroSpec.DataType supportedAvroType = null;
      if ( actualAvroType != null && isSupported( actualAvroType ) ) {
        supportedAvroType = actualAvroType;
      }

      if ( supportedAvroType == null ) {
        // Todo: log a message about skipping unsupported fields
        continue;
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
        case TIMESTAMP_MILLIS:
          pentahoType = ValueMetaInterface.TYPE_TIMESTAMP;
          break;
      }

      // If this is a Pentaho 8 Avro field name, use the ValueMetaInterface type encoded in the Avro field name instead
      FieldName fieldName = parseFieldName( f.name() );
      if ( fieldName != null ) {
        pentahoType = fieldName.type;
      }

      AvroInputField avroInputField = new AvroInputField();
      avroInputField.setFormatFieldName( f.name() );
      avroInputField.setPentahoFieldName( avroInputField.getDisplayableAvroFieldName() );
      avroInputField.setFormatFieldName( f.name() );
      avroInputField.setPentahoType( pentahoType );
      avroInputField.setAvroType( actualAvroType );
      fields.add( avroInputField );
    }

    return fields;
  }

  private AvroSpec.DataType findActualDataType( Schema.Field field ) {
    AvroSpec.DataType avroDataType = null;
    LogicalType logicalType = null;
    Schema.Type primitiveAvroType = null;

    if ( field.schema().getType().equals( Schema.Type.UNION ) ) {
      for ( Schema typeSchema : field.schema().getTypes() ) {
        if ( !typeSchema.getType().equals( Schema.Type.NULL ) ) {
          logicalType = typeSchema.getLogicalType();
          primitiveAvroType = typeSchema.getType();
          break;
        }
      }
    } else {
      logicalType = field.schema().getLogicalType();
      primitiveAvroType = field.schema().getType();
    }

    if ( logicalType != null ) {
      for ( AvroSpec.DataType tmpType : AvroSpec.DataType.values() ) {
        if ( !tmpType.isPrimitiveType() && tmpType.getType().equals( logicalType.getName() ) ) {
          avroDataType = tmpType;
          break;
        }
      }
    } else {
      switch ( primitiveAvroType ) {
        case INT:
          avroDataType = AvroSpec.DataType.INTEGER;
          break;
        case LONG:
          avroDataType = AvroSpec.DataType.LONG;
          break;
        case BYTES:
          avroDataType = AvroSpec.DataType.BYTES;
          break;
        case FLOAT:
          avroDataType = AvroSpec.DataType.FLOAT;
          break;
        case DOUBLE:
          avroDataType = AvroSpec.DataType.DOUBLE;
          break;
        case STRING:
          avroDataType = AvroSpec.DataType.STRING;
          break;
        case BOOLEAN:
          avroDataType = AvroSpec.DataType.BOOLEAN;
          break;
      }
    }

    return avroDataType;
  }

  private boolean isSupported( AvroSpec.DataType actualAvroType ) {
    return ( actualAvroType == AvroSpec.DataType.DATE )
      || ( actualAvroType == AvroSpec.DataType.DECIMAL )
      || ( actualAvroType == AvroSpec.DataType.TIMESTAMP_MILLIS )
      || ( actualAvroType.isPrimitiveType()
           && actualAvroType != AvroSpec.DataType.NULL );
  }

  public static FieldName parseFieldName( String fieldName ) {
    if ( fieldName == null || !fieldName.contains( FieldName.FIELDNAME_DELIMITER ) ) {
      return null;
    }

    String[] splits = fieldName.split( FieldName.FIELDNAME_DELIMITER );

    if ( splits.length == 0 || splits.length > 3 ) {
      return null;
    } else {
      return new FieldName( splits[0], Integer.valueOf( splits[1] ), Boolean.parseBoolean( splits[2] ) );
    }
  }

  /**
   * @deprecated This is only used to read the schema generated using 8.0
   */
  public static class FieldName {
    public final String name;
    public final int type;
    public final boolean allowNull;
    public static final String FIELDNAME_DELIMITER = "_delimiter_";

    public FieldName( String name, int type, boolean allowNull ) {
      this.name = name;
      this.type = type;
      this.allowNull = allowNull;
    }

    public String getLegacyFieldName() {
      return name + FIELDNAME_DELIMITER + type + FIELDNAME_DELIMITER + allowNull;
    }
  }

  public VariableSpace getVariableSpace() {
    return variableSpace;
  }

  @Override
  public void setVariableSpace( VariableSpace variableSpace ) {
    this.variableSpace = variableSpace;
  }

  public void setIncomingFields( Object[] incomingFields ){
    this.incomingFields = incomingFields;
  }

  public Object[] getIncomingFields( ){
    return incomingFields;
  }

  public RowMetaInterface getOutputRowMeta() {
    return outputRowMeta;
  }

  @Override
  public void setOutputRowMeta( RowMetaInterface outputRowMeta ) {
    this.outputRowMeta = outputRowMeta;
  }

  @Override
  public List<? extends IAvroInputField> getLeafFields() throws Exception {
    List<? extends IAvroInputField> inputFields = null;
    Schema s = readAvroSchema();
    inputFields = AvroNestedFieldGetter.getLeafFields( s );
    return inputFields;
  }
}
