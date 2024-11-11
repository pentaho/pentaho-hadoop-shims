/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter;

import parquet.hadoop.ParquetRecordReader;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat.IPentahoRecordReader;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Vasilina_Terehova on 7/29/2017.
 */
public class PentahoParquetRecordReader implements IPentahoRecordReader {

  private final ParquetRecordReader<RowMetaAndData> nativeParquetRecordReader;

  public PentahoParquetRecordReader( ParquetRecordReader<RowMetaAndData> parquetReader ) {
    this.nativeParquetRecordReader = parquetReader;
  }

  @Override
  public void close() throws IOException {
    nativeParquetRecordReader.close();
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
