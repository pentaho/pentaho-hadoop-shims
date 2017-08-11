package org.pentaho.hadoop.shim.common.format;

import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetRecordWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.format.PentahoOutputFormat;
import org.pentaho.hadoop.shim.api.format.PentahoRecordWriter;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;
import org.pentaho.hadoop.shim.common.ConfigurationProxy;

import java.io.IOException;

import static org.pentaho.hadoop.shim.common.format.PentahoParquetInputFormat.JOB_ID;

/**
 * Created by Vasilina_Terehova on 8/3/2017.
 */
public class PentahoParquetOutputFormat implements PentahoOutputFormat {

  private final ConfigurationProxy conf;
  private final ParquetOutputFormat nativeParquetOutputFormat;
  private JobContextImpl jobContext;
  private JobID jobId;
  private TaskAttemptID taskAttemptID;

  public PentahoParquetOutputFormat( Configuration jobConfiguration, SchemaDescription schema ) {

    jobConfiguration.set( ParquetOutputFormat.ENABLE_JOB_SUMMARY, "false" );
    jobConfiguration.set( ParquetOutputFormat.WRITE_SUPPORT_CLASS, ParquetConverter.MyParquetWriteSupport.class
      .getName() );
    jobConfiguration.set( "PentahoParquetSchema", schema.marshall() );
    //        jobConfiguration.set( ParquetInputFormat.SPLIT_MAXSIZE, Long.toString( SPLIT_SIZE ) );

    this.conf = (ConfigurationProxy) jobConfiguration;

    jobId = new JobID( "Job name", JOB_ID );
    jobContext = new JobContextImpl( conf, jobId );
    taskAttemptID = new TaskAttemptID( "qq", 111, TaskType.MAP, 11, 11 );
    nativeParquetOutputFormat = new ParquetOutputFormat<>();
  }

  @Override
  public PentahoRecordWriter getRecordWriter() {
    TaskAttemptContextImpl task = new TaskAttemptContextImpl( conf, taskAttemptID );
    try {
      ParquetRecordWriter<RowMetaAndData> recordWriter =
        (ParquetRecordWriter) nativeParquetOutputFormat.getRecordWriter( task );
      WriteSupport writeSupport = nativeParquetOutputFormat.getWriteSupport( conf );
      return new PentahoParquetRecordWriter( recordWriter, conf, writeSupport, task );
    } catch ( IOException e ) {
      throw new RuntimeException( "some eror accessing parquet files", e );
    } catch ( InterruptedException e ) {
      //logging here
      e.printStackTrace();
      throw new RuntimeException( "This should never happen " + e );
    }
  }

  @Override public Configuration getActiveConfiguration() {
    return conf;
  }
}
