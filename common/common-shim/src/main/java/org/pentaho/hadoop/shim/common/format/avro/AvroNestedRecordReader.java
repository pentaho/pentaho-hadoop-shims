package org.pentaho.hadoop.shim.common.format.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hadoop.shim.api.format.IAvroInputField;
import org.pentaho.hadoop.shim.api.format.IPentahoAvroInputFormat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.Consumer;

public class AvroNestedRecordReader  implements IPentahoAvroInputFormat.IPentahoRecordReader {
  private final DataFileStream<Object> nativeAvroRecordReader;
  private final Schema avroSchema;
  private final List<? extends IAvroInputField> fields;
  private final AvroNestedReader avroNestedReader;
  private final VariableSpace avroInputStep;
  private Object[] incomingFields;
  private RowMetaAndData nextRowBuffer;
  private boolean nextRowStale;
  Object[][] avroOutputExpandedRows;
  private boolean isAvroFile = true; // As opposed to datum. We need to run both ways at some point

  private int outputArraySubscript = 0;

  public AvroNestedRecordReader( DataFileStream<Object> nativeAvroRecordReader,
                                 Schema avroSchema, List<? extends IAvroInputField> fields, VariableSpace avroInputStep,
                                 Object[] incomingFields, RowMetaInterface outputRowMeta ) {

    this.nativeAvroRecordReader = nativeAvroRecordReader;
    this.avroSchema = avroSchema;
    this.fields = fields;
    this.avroInputStep = avroInputStep;
    this.incomingFields = incomingFields;

    avroNestedReader = new AvroNestedReader();
    avroNestedReader.m_schemaToUse = avroSchema;
    avroNestedReader.m_outputRowMeta = outputRowMeta;
    try {
      if ( isAvroFile ) {
        avroNestedReader.m_containerReader = nativeAvroRecordReader;
      } else {
        avroNestedReader.m_datumReader = new GenericDatumReader<Object>( avroSchema );
        avroNestedReader.m_decoder = DecoderFactory.get()
          .jsonDecoder( avroSchema, new ByteArrayInputStream( "input stream goes here".getBytes() ) );
      }

    } catch ( IOException e ) {
      e.printStackTrace();
    }


    ArrayList<AvroInputField> castedList = new ArrayList<AvroInputField>();
    for ( IAvroInputField field : fields ) {
      castedList.add( (AvroInputField) field );
    }
    avroNestedReader.m_normalFields = castedList;
    try {
      avroNestedReader.init();
    } catch ( KettleException e ) {
      e.printStackTrace();
    }
  }

  @Override public void close() throws IOException {

  }

  @Override public Iterator<RowMetaAndData> iterator() {

    return new Iterator<RowMetaAndData>() {

      @Override public boolean hasNext() {

        if ( nextRowBuffer == null || nextRowStale ) {
          return bufferNextRow();
        } else {
          return true;
        }
        //return nativeAvroRecordReader.hasNext();
      }

      @Override public RowMetaAndData next() {
        if ( nextRowBuffer != null ) {
          return nextRowBuffer;
        } else {
          if ( bufferNextRow() ) {
            nextRowStale = true;
            return nextRowBuffer;
          } else {
            throw new NoSuchElementException();
          }
          //return getRowMetaAndData(  nativeAvroRecordReader.next()
        }
      }
    };
  }

  private boolean bufferNextRow() {
    if ( avroOutputExpandedRows == null || outputArraySubscript >= avroOutputExpandedRows.length ) {
      outputArraySubscript = 0;
      try {
        avroOutputExpandedRows = avroNestedReader.avroObjectToKettle( incomingFields, avroInputStep );
        if ( avroOutputExpandedRows.length == 0 ){
          return false;
        }
      } catch ( KettleException e ) {
        e.printStackTrace();
      }
    }

    nextRowBuffer = objectToRowMetaAndData( avroOutputExpandedRows[ outputArraySubscript ] );
    outputArraySubscript++;
    return true;
  }

  private RowMetaAndData objectToRowMetaAndData( Object[] rawRow ) {
    return null;
  }

  @Override public Spliterator<RowMetaAndData> spliterator() {
    return null;
  }
}
