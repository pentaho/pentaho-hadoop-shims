/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import parquet.hadoop.ParquetRecordWriter;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat.IPentahoRecordWriter;

import java.io.IOException;

/**
 * Created by Vasilina_Terehova on 8/3/2017.
 */
public class PentahoParquetRecordWriter implements IPentahoRecordWriter {
  private final ParquetRecordWriter<RowMetaAndData> nativeParquetRecordWriter;

  private final TaskAttemptContext taskAttemptContext;

  public PentahoParquetRecordWriter( ParquetRecordWriter<RowMetaAndData> recordWriter,
                                     TaskAttemptContext taskAttemptContext ) {
    this.nativeParquetRecordWriter = recordWriter;

    this.taskAttemptContext = taskAttemptContext;
  }

  @Override
  public void write( RowMetaAndData row ) {
    try {
      nativeParquetRecordWriter.write( null, row );
    } catch ( IOException e ) {
      throw new IllegalArgumentException( "some exception while writing ", e );
    } catch ( InterruptedException e ) {
      throw new IllegalArgumentException( "interrupted exception writing parquet ", e );
    }
  }

  @Override
  public void close() throws IOException {
    try {
      nativeParquetRecordWriter.close( taskAttemptContext );
    } catch ( InterruptedException e ) {
      throw new IllegalArgumentException( "interrupted exception writing parquet ", e );
    }
  }
}
