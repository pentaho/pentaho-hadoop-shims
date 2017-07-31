package org.pentaho.hadoop.shim.common.format;


import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.format.InputFormat;
import org.pentaho.hadoop.shim.api.format.PentahoInputSplit;
import org.pentaho.hadoop.shim.api.format.RecordReader;
import org.pentaho.hadoop.shim.api.fs.FileSystem;
import org.pentaho.hadoop.shim.common.ConfigurationProxy;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Vasilina_Terehova on 7/25/2017.
 */
public class PentahoParquetInputFormat implements InputFormat {

  public static final int JOB_ID = Integer.MAX_VALUE;
  private ConfigurationProxy conf;
  private ParquetInputFormat<String> nativeParquetInputFormat;
  private JobContextImpl jobContext;
  private JobID jobId;
  private TaskAttemptID taskAttemptID;

  public PentahoParquetInputFormat( Configuration jobConfiguration, FileSystem path, String schema ) {
    //make builder for configuration to set base params
    jobConfiguration.set( ParquetInputFormat.SPLIT_MAXSIZE, "10000000" );
    jobConfiguration.set( ParquetInputFormat.TASK_SIDE_METADATA, "false" );
    jobConfiguration.set( ParquetInputFormat.READ_SUPPORT_CLASS, GroupReadSupport.class.getName() );
    this.conf = (ConfigurationProxy) jobConfiguration;

    jobId = new JobID( "Job name", JOB_ID);
    jobContext = new JobContextImpl( conf, jobId );
    taskAttemptID = new TaskAttemptID();
    nativeParquetInputFormat = new ParquetInputFormat<>();
  }

  @Override public List<PentahoInputSplit> getSplits() throws IOException {
    return nativeParquetInputFormat.getSplits( jobContext ).stream().map(PentahoInputSplitImpl::new).collect(Collectors.toList());
  }

  //for parquet not actual to point split
  @Override public RecordReader getRecordReader( PentahoInputSplit split ) throws IOException, InterruptedException {
    TaskAttemptContextImpl task = new TaskAttemptContextImpl( conf, taskAttemptID );
    PentahoInputSplitImpl pentahoInputSplit = (PentahoInputSplitImpl) split;
    InputSplit inputSplit = pentahoInputSplit.getInputSplit();
    ParquetRecordReader rd = (ParquetRecordReader) nativeParquetInputFormat.createRecordReader(inputSplit, task );
    rd.initialize( inputSplit, task );
    return new PentahoParquetRecordReader( nativeParquetInputFormat, rd, jobContext );
  }
}
