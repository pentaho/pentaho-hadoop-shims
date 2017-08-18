/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.hadoop.shim.api.format.RecordReader;

import java.io.IOException;
import java.util.Iterator;

//#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI" || shim_type=="MAPR"
//#endif
//#if shim_type=="CDH"
//$import parquet.hadoop.ParquetInputFormat;
//$import parquet.hadoop.ParquetRecordReader;
//#endif

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
