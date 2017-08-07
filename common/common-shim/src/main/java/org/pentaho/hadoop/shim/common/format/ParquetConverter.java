package org.pentaho.hadoop.shim.common.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
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

/**
 * Converter for read/write Pentaho row from/into Parquet files.
 * 
 * @author Alexander Buloichik
 */
public class ParquetConverter {
  private final SchemaDescription schema;

  public ParquetConverter( SchemaDescription schema ) {
    this.schema = schema;
  }

  public MessageType createParquetSchema() {
    List<Type> types = new ArrayList<>();

    schema.forEach( f -> types.add( convertField( f ) ) );

    return new MessageType( "parquet-scema", types );
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

  private void writeValue( SchemaDescription.Field field, RowMetaAndData row, RecordConsumer consumer )
    throws KettleValueException {
    switch ( field.pentahoValueMetaType ) {
      case ValueMetaInterface.TYPE_NUMBER:
        consumer.addDouble( row.getNumber( field.pentahoFieldName, Double.parseDouble( field.defaultValue ) ) );
      case ValueMetaInterface.TYPE_STRING:
        consumer.addBinary( Binary.fromString( row.getString( field.pentahoFieldName, field.defaultValue ) ) );
        break;
      case ValueMetaInterface.TYPE_BOOLEAN:
        consumer.addBoolean( row.getBoolean( field.pentahoFieldName, Boolean.parseBoolean( field.defaultValue ) ) );
        break;
      case ValueMetaInterface.TYPE_INTEGER:
        consumer.addLong( row.getInteger( field.pentahoFieldName, 0l ) );
        break;
      case ValueMetaInterface.TYPE_BIGNUMBER:
        consumer.addDouble( row.getNumber( field.pentahoFieldName, Double.parseDouble( field.defaultValue ) ) );
        break;
      case ValueMetaInterface.TYPE_SERIALIZABLE:
        consumer.addBinary( Binary.fromReusedByteArray( row.getBinary( field.pentahoFieldName, new byte[0] ) ) );
        break;
      case ValueMetaInterface.TYPE_BINARY:
        consumer.addBinary( Binary.fromReusedByteArray( row.getBinary( field.pentahoFieldName, new byte[0] ) ) );
        break;
      default:
        throw new RuntimeException( "Undefined type: " + field.pentahoValueMetaType );
    }
  }

  public void writeRow( RowMetaAndData row, RecordConsumer consumer ) {
    consumer.startMessage();
    for ( SchemaDescription.Field f : schema ) {
      if ( f.formatFieldName == null ) {
        continue;
      }
      consumer.startField( f.formatFieldName, 0 );
      try {
        writeValue( f, row, consumer );
      } catch ( KettleValueException ex ) {
        throw new RuntimeException( ex );
      }
      consumer.endField( f.formatFieldName, 0 );
    }
    consumer.endMessage();
  }

  public RowMetaAndData readRow( RecordReader reader ) throws IOException, InterruptedException {
    RowMeta rowMeta = new RowMeta();
    List<Object> data = new ArrayList<>();

    while ( reader.nextKeyValue() ) {
      Object o = reader.getCurrentValue();
      System.out.println( o );
    }

    return new RowMetaAndData( rowMeta, data.toArray( new Object[data.size()] ) );
  }

  public static class MyParquetWriteSupport extends WriteSupport<RowMetaAndData> {
    ParquetConverter converter;
    RecordConsumer consumer;

    @Override
    public WriteContext init( Configuration configuration ) {
      String schemaStr = configuration.get( "PentahoParquetSchema" );
      if ( schemaStr == null ) {
        throw new RuntimeException( "Schema not defined in the PentahoParquetSchema key" );
      }
      converter = new ParquetConverter( SchemaDescription.unmarshall( schemaStr ) );

      try {
        WriteContext wc = new WriteContext( converter.createParquetSchema(), new TreeMap<>() );
        return wc;
      } catch ( Exception ex ) {
        throw new RuntimeException( ex );
      }
    }

    @Override
    public void prepareForWrite( RecordConsumer recordConsumer ) {
      consumer = recordConsumer;
    }

    @Override
    public void write( RowMetaAndData record ) {
      converter.writeRow( record, consumer );
    }
  }

  public static class MyParquetReadSupport extends ReadSupport<RowMetaAndData> {
    ParquetConverter converter;

    @Override
    public ReadContext init( InitContext context ) {

      String schemaStr = context.getConfiguration().get( "PentahoParquetSchema" );
      if ( schemaStr == null ) {
        throw new RuntimeException( "Schema not defined in the PentahoParquetSchema key" );
      }
      converter = new ParquetConverter( SchemaDescription.unmarshall( schemaStr ) );

      System.out.println( context.getFileSchema() );

      return new ReadContext( converter.createParquetSchema(), new HashMap<String, String>() );
    }

    @Override
    public RecordMaterializer<RowMetaAndData> prepareForRead( Configuration configuration,
        Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext ) {
      return new MyRecordMaterializer( converter );
    }
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
