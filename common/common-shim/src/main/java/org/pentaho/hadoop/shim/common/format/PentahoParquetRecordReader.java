package org.pentaho.hadoop.shim.common.format;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.hadoop.shim.api.format.RecordReader;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Vasilina_Terehova on 7/29/2017.
 */
public class PentahoParquetRecordReader implements RecordReader {

  private final ParquetRecordReader nativeParquetRecordReader;
  private final ParquetInputFormat nativeParquetInputFormat;
  private final JobContext jobContext;

  public PentahoParquetRecordReader( ParquetInputFormat parquetInputFormat, ParquetRecordReader parquetReader,
                                     JobContext jobContext ) {
    this.nativeParquetInputFormat = parquetInputFormat;
    this.nativeParquetRecordReader = parquetReader;
    this.jobContext = jobContext;
  }

  @Override public void close() throws IOException {

  }

  @Override public Iterator<RowMetaAndData> iterator() {

    return new Iterator<RowMetaAndData>() {
      @Override public boolean hasNext() {
        try {
          return nativeParquetRecordReader.nextKeyValue();
        } catch ( IOException e ) {
          throw new IllegalArgumentException( "some error while reading parquet file", e );
        } catch ( InterruptedException e ) {
          //this should never happen
          throw new IllegalArgumentException( "sync error while reading parquet file", e );
        }
      }

      @Override public RowMetaAndData next() {
        RowMetaAndData rowMetaAndData = new RowMetaAndData();
        try {
          MessageType messageType = nativeParquetInputFormat.getGlobalMetaData( jobContext ).getSchema();

          Group currentValue = (Group) nativeParquetRecordReader.getCurrentValue();
          //scheme
          for ( ColumnDescriptor columnDescriptor : messageType.getColumns() ) {
            if ( columnDescriptor.getType() == PrimitiveType.PrimitiveTypeName.INT64 ) {
              //path is arr->field - path to field
              //index - field can be array
              String fieldName = columnDescriptor.getPath()[ 0 ];
              Long aLong = currentValue.getLong( fieldName, 0 );
              rowMetaAndData.addValue( new ValueMetaInteger( fieldName ), aLong );
            } else {
              //path is arr->field - path to field
              //index - field can be array
              String fieldName = columnDescriptor.getPath()[ 0 ];
              String string = currentValue.getString( fieldName, 0 );
              rowMetaAndData.addValue( new ValueMetaString( fieldName ), string );
            }
          }
          //return (RowMetaAndData) currentValue;
        } catch ( IOException e ) {
          throw new IllegalArgumentException( "some error while reading parquet file", e );
        } catch ( InterruptedException e ) {
          //this should never happen
          throw new IllegalArgumentException( "sync error while reading parquet file", e );
        }
        return rowMetaAndData;
      }
    };
  }
}
