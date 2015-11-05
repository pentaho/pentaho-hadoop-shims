package org.pentaho.hadoop.shim.common;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunner;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.common.fs.PathProxy;
import org.pentaho.hbase.mapred.PentahoTableInputFormat;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.*;
import static org.mockito.Matchers.contains;
import static org.mockito.Matchers.endsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

public class ConfigurationProxyTest {

  private ConfigurationProxy configurationProxy;

  @Before
  public void setUp() throws Exception {
    configurationProxy = new ConfigurationProxy();
  }

  @Test
  public void testSetMapperClass() throws Exception {
    configurationProxy.setMapperClass( Mapper.class );
    assertEquals( Mapper.class, configurationProxy.getMapperClass() );
  }

  @Test
  public void testSetCombinerClass() throws Exception {
    configurationProxy.setCombinerClass( Reducer.class );
    assertEquals( Reducer.class, configurationProxy.getCombinerClass() );
  }

  @Test
  public void testSetReducerClass() throws Exception {
    configurationProxy.setReducerClass( Reducer.class );
    assertEquals( Reducer.class, configurationProxy.getReducerClass() );
  }

  @Test
  public void testSetMapRunnerClass() throws Exception {
    configurationProxy.setMapRunnerClass( MapRunner.class );
    assertEquals( MapRunner.class, configurationProxy.getMapRunnerClass() );
  }

  @Test
  public void testSetInputFormat() throws Exception {
    configurationProxy.setInputFormat( TestInputFormat.class );
    assertEquals( TestInputFormat.class, configurationProxy.getInputFormat().getClass() );
  }

  @Test
  public void testSetOutputFormat() throws Exception {
    configurationProxy.setOutputFormat( TestOutputFormat.class );
    assertEquals( TestOutputFormat.class, configurationProxy.getOutputFormat().getClass() );
  }

  @Test
  public void testGetDefaultFileSystemURL() throws Exception {
    String defaultFileSystemURL = configurationProxy.getDefaultFileSystemURL();
    assertEquals( defaultFileSystemURL, configurationProxy.get( "fs.default.name", "" ) );
  }

  @Test
  public void testGetAsDelegateConf() throws Exception {
    assertNull( configurationProxy.getAsDelegateConf( String.class ) );
    assertEquals( configurationProxy, configurationProxy.getAsDelegateConf( ConfigurationProxy.class ) );
  }

  @Test
  public void testSetInputPaths() throws Exception {
    configurationProxy.setInputPaths( null );
    Path[] inputPaths = FileInputFormat.getInputPaths( configurationProxy );
    assertEquals( 0, inputPaths.length );

    PathProxy path1 = new PathProxy( "file://path1" );
    PathProxy path2 = new PathProxy( "file://path2" );
    configurationProxy.setInputPaths( path1, path2);

    inputPaths = FileInputFormat.getInputPaths( configurationProxy );
    assertEquals( 2, inputPaths.length );
    assertArrayEquals( new Path[] {path1, path2}, inputPaths );
  }

  @Test
  public void testSetOutputPath() throws Exception {
    PathProxy path = new PathProxy( "file://output" );
    configurationProxy.setOutputPath( path );
    assertEquals( path, FileOutputFormat.getOutputPath( configurationProxy ) );
  }

  static class TestInputFormat implements InputFormat {
    @Override
    public InputSplit[] getSplits( JobConf jobConf, int i ) throws IOException {
      return new InputSplit[0];
    }

    @Override
    public RecordReader getRecordReader( InputSplit inputSplit, JobConf jobConf, Reporter reporter )
        throws IOException {
      return null;
    }
  }

  static class TestOutputFormat implements OutputFormat {

    @Override
    public RecordWriter getRecordWriter( FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable )
        throws IOException {
      return null;
    }

    @Override
    public void checkOutputSpecs( FileSystem fileSystem, JobConf jobConf ) throws IOException {

    }
  }
}
