package org.pentaho.hadoop.shim.common.format;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
//#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI" || shim_type=="MAPR"
import org.apache.parquet.hadoop.ParquetRecordWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
//#endif

//#if shim_type=="CDH"
//$import parquet.hadoop.ParquetRecordWriter;
//$import parquet.hadoop.api.WriteSupport;
//#endif

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.hadoop.shim.api.format.PentahoRecordWriter;
import org.pentaho.hadoop.shim.common.ConfigurationProxy;

import java.io.IOException;

/**
 * Created by Vasilina_Terehova on 8/3/2017.
 */
public class PentahoParquetRecordWriter implements PentahoRecordWriter {
  private final ParquetRecordWriter<RowMetaAndData> nativeParquetRecordWriter;
  private final ConfigurationProxy configuration;
  private final WriteSupport writeSupport;
  private final TaskAttemptContext taskAttemptContext;

  public PentahoParquetRecordWriter( ParquetRecordWriter<RowMetaAndData> recordWriter, ConfigurationProxy configuration,
                                     WriteSupport writeSupport, TaskAttemptContext taskAttemptContext ) {
    this.nativeParquetRecordWriter = recordWriter;
    this.configuration = configuration;
    this.writeSupport = writeSupport;
    this.taskAttemptContext = taskAttemptContext;
  }

  @Override
  public void write( RowMetaAndData row ) {
    Void key = null;
    try {
      nativeParquetRecordWriter.write( key, row );
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
