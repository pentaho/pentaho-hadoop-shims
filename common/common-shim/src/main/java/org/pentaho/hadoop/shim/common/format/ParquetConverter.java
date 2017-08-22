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
package org.pentaho.hadoop.shim.common.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.RecordReader;

//#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI" || shim_type=="MAPR"
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
//#endif
//#if shim_type=="CDH"
//$import parquet.io.api.Binary;
//$import parquet.io.api.Converter;
//$import parquet.io.api.GroupConverter;
//$import parquet.io.api.PrimitiveConverter;
//$import parquet.io.api.RecordConsumer;
//$import parquet.io.api.RecordMaterializer;
//$import parquet.schema.MessageType;
//$import parquet.schema.PrimitiveType;
//$import parquet.schema.PrimitiveType.PrimitiveTypeName;
//$import parquet.schema.Type;
//$import parquet.schema.Type.Repetition;
//#endif

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBigNumber;
import org.pentaho.di.core.row.value.ValueMetaBinary;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaSerializable;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

/**
 * Converter for read/write Pentaho row from/into Parquet files.
 * 
 * @author Alexander Buloichik
 */
public class ParquetConverter {
  public static final int PARQUET_JOB_ID = Integer.MAX_VALUE;
  public static final String PARQUET_SCHEMA_CONF_KEY = "PentahoParquetSchema";

  private final SchemaDescription schema;

  public ParquetConverter( SchemaDescription schema ) {
    this.schema = schema;
  }

  public MessageType createParquetSchema() {
    List<Type> types = new ArrayList<>();

    schema.forEach( f -> types.add( convertField( f ) ) );

    return new MessageType( "parquet-schema", types );
  }

  private PrimitiveType convertField( SchemaDescription.Field f ) {
    Repetition rep = f.allowNull ? Repetition.OPTIONAL : Repetition.REQUIRED;
    switch ( f.pentahoValueMetaType ) {
      case ValueMetaInterface.TYPE_NUMBER:
        return new PrimitiveType( rep, PrimitiveTypeName.DOUBLE, f.formatFieldName );
      case ValueMetaInterface.TYPE_STRING:
        return new PrimitiveType( rep, PrimitiveTypeName.BINARY, f.formatFieldName );
      case ValueMetaInterface.TYPE_BOOLEAN:
        return new PrimitiveType( rep, PrimitiveTypeName.BOOLEAN, f.formatFieldName );
      case ValueMetaInterface.TYPE_INTEGER:
        return new PrimitiveType( rep, PrimitiveTypeName.INT64, f.formatFieldName );
      case ValueMetaInterface.TYPE_BIGNUMBER:
        return new PrimitiveType( rep, PrimitiveTypeName.DOUBLE, f.formatFieldName );
      case ValueMetaInterface.TYPE_SERIALIZABLE:
        return new PrimitiveType( rep, PrimitiveTypeName.BINARY, f.formatFieldName );
      case ValueMetaInterface.TYPE_BINARY:
        return new PrimitiveType( rep, PrimitiveTypeName.BINARY, f.formatFieldName );
      default:
        throw new RuntimeException( "Undefined type: " + f.pentahoValueMetaType );
    }
  }

  private void writeField( SchemaDescription.Field field, RowMetaAndData row, RecordConsumer consumer )
    throws KettleValueException {
    int fieldIndex = row.getRowMeta().indexOfValue( field.pentahoFieldName );
    if ( fieldIndex < 0 ) {
      return;
    }
    if ( row.isEmptyValue( field.pentahoFieldName ) ) {
      return;
    }
    consumer.startField( field.formatFieldName, 0 );
    switch ( field.pentahoValueMetaType ) {
      case ValueMetaInterface.TYPE_NUMBER:
        consumer.addDouble( row.getNumber( fieldIndex, Double.parseDouble( field.defaultValue ) ) );
      case ValueMetaInterface.TYPE_STRING:
        consumer.addBinary( Binary.fromString( row.getString( field.pentahoFieldName, field.defaultValue ) ) );
        break;
      case ValueMetaInterface.TYPE_BOOLEAN:
        consumer.addBoolean( row.getBoolean( fieldIndex, Boolean.parseBoolean( field.defaultValue ) ) );
        break;
      case ValueMetaInterface.TYPE_INTEGER:
        consumer.addLong( row.getInteger( fieldIndex, Long.parseLong( field.defaultValue ) ) );
        break;
      case ValueMetaInterface.TYPE_BIGNUMBER:
        consumer.addDouble( row.getNumber( fieldIndex, Double.parseDouble( field.defaultValue ) ) );
        break;
      case ValueMetaInterface.TYPE_SERIALIZABLE:
        consumer.addBinary( Binary.fromReusedByteArray( row.getBinary( fieldIndex, new byte[0] ) ) );
        break;
      case ValueMetaInterface.TYPE_BINARY:
        consumer.addBinary( Binary.fromReusedByteArray( row.getBinary( fieldIndex, new byte[0] ) ) );
        break;
      default:
        throw new RuntimeException( "Undefined type: " + field.pentahoValueMetaType );
    }
    consumer.endField( field.formatFieldName, 0 );
  }

  public void writeRow( RowMetaAndData row, RecordConsumer consumer ) {
    consumer.startMessage();
    for ( SchemaDescription.Field f : schema ) {
      if ( f.formatFieldName == null ) {
        continue;
      }
      try {
        writeField( f, row, consumer );
      } catch ( KettleValueException ex ) {
        throw new RuntimeException( ex );
      }
    }
    consumer.endMessage();
  }

  public RowMetaAndData readRow( RecordReader<Void, RowMetaAndData> reader ) throws IOException, InterruptedException {
    RowMeta rowMeta = new RowMeta();
    List<Object> data = new ArrayList<>();

    while ( reader.nextKeyValue() ) {
      Object o = reader.getCurrentValue();
      System.out.println( o );
    }

    return new RowMetaAndData( rowMeta, data.toArray( new Object[data.size()] ) );
  }

  public static class MyRecordMaterializer extends RecordMaterializer<RowMetaAndData> {
    private MyGroupConverter root;

    public MyRecordMaterializer( ParquetConverter converter ) {
      root = new MyGroupConverter( converter );
    }

    @Override
    public RowMetaAndData getCurrentRecord() {
      return root.getCurrentRecord();
    }

    @Override
    public GroupConverter getRootConverter() {
      return root;
    }
  }

  public static class MyGroupConverter extends GroupConverter {
    protected RowMetaAndData current;
    private Converter[] converters;

    public MyGroupConverter( ParquetConverter converter ) {
      int count = 0;
      for ( SchemaDescription.Field f : converter.schema ) {
        if ( f.formatFieldName != null ) {
          count++;
        }
      }
      converters = new Converter[count];
      int i = 0;
      for ( SchemaDescription.Field f : converter.schema ) {
        if ( f.formatFieldName == null ) {
          continue;
        }

        switch ( f.pentahoValueMetaType ) {
          case ValueMetaInterface.TYPE_NUMBER:
            converters[i] = new PrimitiveConverter() {
              @Override
              public void addDouble( double value ) {
                current.addValue( new ValueMetaNumber( f.pentahoFieldName ), value );
              }

              @Override
              public void addFloat( float value ) {
                current.addValue( new ValueMetaNumber( f.pentahoFieldName ), value );
              }

              @Override
              public void addInt( int value ) {
                current.addValue( new ValueMetaNumber( f.pentahoFieldName ), value );
              }

              @Override
              public void addLong( long value ) {
                current.addValue( new ValueMetaNumber( f.pentahoFieldName ), value );
              }
            };
            break;
          case ValueMetaInterface.TYPE_INTEGER:
            converters[i] = new PrimitiveConverter() {
              @Override
              public void addDouble( double value ) {
                current.addValue( new ValueMetaInteger( f.pentahoFieldName ), value );
              }

              @Override
              public void addFloat( float value ) {
                current.addValue( new ValueMetaInteger( f.pentahoFieldName ), value );
              }

              @Override
              public void addInt( int value ) {
                current.addValue( new ValueMetaInteger( f.pentahoFieldName ), value );
              }

              @Override
              public void addLong( long value ) {
                current.addValue( new ValueMetaInteger( f.pentahoFieldName ), value );
              }
            };
            break;
          case ValueMetaInterface.TYPE_BIGNUMBER:
            converters[i] = new PrimitiveConverter() {
              @Override
              public void addDouble( double value ) {
                current.addValue( new ValueMetaBigNumber( f.pentahoFieldName ), value );
              }

              @Override
              public void addFloat( float value ) {
                current.addValue( new ValueMetaBigNumber( f.pentahoFieldName ), value );
              }

              @Override
              public void addInt( int value ) {
                current.addValue( new ValueMetaBigNumber( f.pentahoFieldName ), value );
              }

              @Override
              public void addLong( long value ) {
                current.addValue( new ValueMetaBigNumber( f.pentahoFieldName ), value );
              }
            };
            break;
          case ValueMetaInterface.TYPE_STRING:
            converters[i] = new PrimitiveConverter() {
              @Override
              public void addBinary( Binary value ) {
                current.addValue( new ValueMetaString( f.pentahoFieldName ), value.toStringUsingUTF8() );
              }
            };
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            converters[i] = new PrimitiveConverter() {
              @Override
              public void addBoolean( boolean value ) {
                current.addValue( new ValueMetaBoolean( f.pentahoFieldName ), value );
              }
            };
            break;
          case ValueMetaInterface.TYPE_SERIALIZABLE:
            converters[i] = new PrimitiveConverter() {
              @Override
              public void addBinary( Binary value ) {
                current.addValue( new ValueMetaSerializable( f.pentahoFieldName ), value.getBytes() );
              }
            };
            break;
          case ValueMetaInterface.TYPE_BINARY:
            converters[i] = new PrimitiveConverter() {
              @Override
              public void addBinary( Binary value ) {
                current.addValue( new ValueMetaBinary( f.pentahoFieldName ), value.getBytes() );
              }
            };
            break;
          default:
            throw new RuntimeException( "Undefined type: " + f.pentahoValueMetaType );
        }
        i++;
      }
    }

    @Override
    public void start() {
      current = new RowMetaAndData();
    }

    @Override
    public Converter getConverter( int fieldIndex ) {
      return converters[fieldIndex];
    }

    @Override
    public void end() {
      System.out.println( "rec=" + current );
    }

    public RowMetaAndData getCurrentRecord() {
      return current;
    }
  }
}
