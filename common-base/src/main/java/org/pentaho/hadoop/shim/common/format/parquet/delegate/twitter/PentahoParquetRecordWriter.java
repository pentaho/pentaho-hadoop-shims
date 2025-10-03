/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.hadoop.ParquetRecordWriter;
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
