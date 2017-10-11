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
package org.pentaho.hadoop.shim.common.format.parquet;

import java.io.IOException;
import java.util.Iterator;

//#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI"
import org.apache.parquet.hadoop.ParquetRecordReader;
//#endif
//#if shim_type=="CDH" || shim_type=="MAPR"
//$import parquet.hadoop.ParquetRecordReader;
//#endif

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat.IPentahoRecordReader;

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
