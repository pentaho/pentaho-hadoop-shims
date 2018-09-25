package org.pentaho.hadoop.shim.common.format.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.vfs2.FileObject;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.hadoop.shim.api.format.IAvroInputField;
import org.pentaho.hadoop.shim.api.format.IPentahoAvroInputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AvroNestedRecordReader implements IPentahoAvroInputFormat.IPentahoRecordReader {
  private final DataFileStream<Object> nativeAvroRecordReader;
  private final Schema avroSchema;
  private final List<? extends IAvroInputField> fields;
  private final AvroNestedReader avroNestedReader;
  private final VariableSpace avroInputStep;
  private Object[] incomingFields;
  private RowMetaAndData nextRow;
  Object[][] expandedRows = null;

  private int nextExpandedRow = 0;

  public AvroNestedRecordReader( DataFileStream<Object> nativeAvroRecordReader,
                                 Schema avroSchema, List<? extends IAvroInputField> fields, VariableSpace avroInputStep,
                                 Object[] incomingFields, RowMetaInterface outputRowMeta,
                                 String fileName, boolean isDataBinaryEncoded, boolean isDecodingFromField ) {

    this.nativeAvroRecordReader = nativeAvroRecordReader;
    this.avroSchema = avroSchema;
    this.fields = fields;
    this.avroInputStep = avroInputStep;
    this.incomingFields = incomingFields;

    avroNestedReader = new AvroNestedReader();
    avroNestedReader.m_schemaToUse = avroSchema;
    avroNestedReader.m_outputRowMeta = outputRowMeta;
    avroNestedReader.m_jsonEncoded = !isDataBinaryEncoded;
    avroNestedReader.m_decodingFromField = isDecodingFromField;
    avroNestedReader.m_fieldToDecodeIndex = nextExpandedRow;

    try {
      if ( nativeAvroRecordReader != null ) { // Is Avro File
        avroNestedReader.m_containerReader = nativeAvroRecordReader;
      } else {

        if ( avroSchema != null ) {
          avroNestedReader.m_datumReader = new GenericDatumReader<Object>( avroSchema );
          if ( fileName != null ) {
            FileObject fileObject = KettleVFS.getFileObject( fileName );
            avroNestedReader.m_decoder = DecoderFactory.get().jsonDecoder( avroSchema, KettleVFS.getInputStream( fileObject ) );
          }
        }
      }

    } catch ( Exception e ) {
      e.printStackTrace();
    }

    ArrayList<AvroInputField> castedList = new ArrayList<AvroInputField>();
    for ( IAvroInputField field : fields ) {
      AvroInputField newField = new AvroInputField();
      newField.setAvroFieldName( field.getAvroFieldName() );
      newField.setAvroType( field.getAvroType() );
      newField.setIndexedValues( field.getIndexedValues() );
      newField.setFormatFieldName( field.getFormatFieldName() );
      newField.setFormatType( field.getFormatType() );
      newField.setPentahoFieldName( field.getPentahoFieldName() );
      newField.setPentahoType( field.getPentahoType() );
      newField.setPrecision( field.getPrecision() );
      newField.setScale( field.getScale() );
      newField.setStringFormat( field.getStringFormat() );
      castedList.add( newField );
    }

    avroNestedReader.m_normalFields = castedList;
    try {
      avroNestedReader.init();
    } catch ( KettleException e ) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() throws IOException {

  }

  private boolean hasExpandedRows() {
    if ( expandedRows != null ) {
      if( nextExpandedRow < expandedRows.length) {
        return true;
      } else {
        incomingFields = null;
      }
    }
    return false;
  }

  @Override
  public Iterator<RowMetaAndData> iterator() {

    return new Iterator<RowMetaAndData>() {

      @Override
      public boolean hasNext() {
        if ( hasExpandedRows() ) {
          return true;
        }
        if ( nativeAvroRecordReader != null && nativeAvroRecordReader.hasNext() ) {
          return true;
        }
        if ( incomingFields != null ) {
          return true;
        }
        return false;
      }

      @Override
      public RowMetaAndData next() {
        return getNextRowMetaAndData();
      }
    };
  }

  private RowMetaAndData getNextRowMetaAndData() {
    if ( hasExpandedRows() == false ) {
      try {
        nextExpandedRow = 0;
        expandedRows = null;
        expandedRows = avroNestedReader.avroObjectToKettle( incomingFields, avroInputStep );
        if ( expandedRows != null ) {
          nextRow = objectToRowMetaAndData( expandedRows[nextExpandedRow] );
        } else {
          return null;
        }
      } catch ( KettleException e ) {
        e.printStackTrace();
      }
    }

    nextRow = objectToRowMetaAndData( expandedRows[ nextExpandedRow ] );
    nextExpandedRow++;
    return nextRow;
  }

  private RowMetaAndData objectToRowMetaAndData( Object[] row ) {
    RowMetaAndData rowMetaAndData = new RowMetaAndData();
    int index = 0;
    for ( IAvroInputField metaField : fields ) {
      rowMetaAndData.addValue( metaField.getPentahoFieldName(), metaField.getPentahoType(), row[ index ] );
      String stringFormat = metaField.getStringFormat();
      if ( ( stringFormat != null ) && ( stringFormat.trim().length() > 0 ) ) {
        rowMetaAndData.getValueMeta( rowMetaAndData.size() - 1 ).setConversionMask( stringFormat );
      }
      index++;
    }

    return rowMetaAndData;
  }
}
