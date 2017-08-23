package org.pentaho.hadoop.shim.common.format;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.hadoop.shim.api.format.RecordReader;

/**
 * Created by Vasilina_Terehova on 7/29/2017.
 */
public class PentahoParquetRecordReader implements RecordReader {

  private final ParquetRecordReader<RowMetaAndData> nativeParquetRecordReader;
  private final ParquetInputFormat<RowMetaAndData> nativeParquetInputFormat;
  private final JobContext jobContext;

  public PentahoParquetRecordReader( ParquetInputFormat parquetInputFormat, ParquetRecordReader parquetReader,
      JobContext jobContext ) {
    this.nativeParquetInputFormat = parquetInputFormat;
    this.nativeParquetRecordReader = parquetReader;
    this.jobContext = jobContext;
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public Iterator<RowMetaAndData> iterator() {

    return new Iterator<RowMetaAndData>() {
      @Override
      public boolean hasNext() {
        try {
          return nativeParquetRecordReader.nextKeyValue();
        } catch ( IOException e ) {
          throw new IllegalArgumentException( "some error while reading parquet file", e );
        } catch ( InterruptedException e ) {
          // this should never happen
          throw new IllegalArgumentException( "sync error while reading parquet file", e );
        }
      }

      @Override
      public RowMetaAndData next() {
        try {
          return nativeParquetRecordReader.getCurrentValue();
        } catch ( IOException e ) {
          throw new IllegalArgumentException( "some error while reading parquet file", e );
        } catch ( InterruptedException e ) {
          // this should never happen
          throw new IllegalArgumentException( "sync error while reading parquet file", e );
        }
        // return rowMetaAndData;
      }
    };
  }
}
