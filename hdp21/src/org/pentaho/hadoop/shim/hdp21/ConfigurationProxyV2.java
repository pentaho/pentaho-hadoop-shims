/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2014 by Pentaho : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.hdp21;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.common.ShimUtils;

import java.io.IOException;

/**
 * User: Dzmitry Stsiapanau Date: 7/22/14 Time: 11:59 AM
 */
public class ConfigurationProxyV2 implements Configuration {

  private Job job;

  public ConfigurationProxyV2() throws IOException {
    job = Job.getInstance();
    getJobConf().addResource( "hdfs-site.xml" );
  }

  public JobConf getJobConf() {
    return (JobConf) job.getConfiguration();
  }

  public Job getJob() {
    return job;
  }

  /**
   * Sets the MapReduce job name.
   *
   * @param jobName Name of job
   */
  @Override
  public void setJobName( String jobName ) {
    job.setJobName( jobName );
  }

  /**
   * Sets the property {@code name}'s value to {@code value}.
   *
   * @param name  Name of property
   * @param value Value of property
   */
  @Override
  public void set( String name, String value ) {
    getJobConf().set( name, value );
  }

  /**
   * Look up the value of a property.
   *
   * @param name Name of property
   * @return Value of the property named {@code name}
   */
  @Override
  public String get( String name ) {
    return getJobConf().get( name );
  }

  /**
   * Look up the value of a property optionally returning a default value if the property is not set.
   *
   * @param name         Name of property
   * @param defaultValue Value to return if the property is not set
   * @return Value of property named {@code name} or {@code defaultValue} if {@code name} is not set
   */
  @Override
  public String get( String name, String defaultValue ) {
    return getJobConf().get( name, defaultValue );
  }

  /**
   * Set the key class for the map output data.
   *
   * @param c the map output key class
   */
  @Override
  public void setMapOutputKeyClass( Class<?> c ) {
    job.setMapOutputKeyClass( c );
  }

  /**
   * Set the value class for the map output data.
   *
   * @param c the map output value class
   */
  @Override
  public void setMapOutputValueClass( Class<?> c ) {
    job.setMapOutputValueClass( c );
  }

  @SuppressWarnings( "unchecked" )
  @Override
  public void setMapperClass( Class<?> c ) {
    if ( org.apache.hadoop.mapred.Mapper.class.isAssignableFrom( c ) ) {
      getJobConf().setMapperClass( (Class<? extends org.apache.hadoop.mapred.Mapper>) c );
    } else if ( org.apache.hadoop.mapreduce.Mapper.class.isAssignableFrom( c ) ) {
      job.setMapperClass( (Class<? extends org.apache.hadoop.mapreduce.Mapper>) c );
    }
  }

  @SuppressWarnings( "unchecked" )
  @Override
  public void setCombinerClass( Class<?> c ) {
    if ( org.apache.hadoop.mapred.Reducer.class.isAssignableFrom( c ) ) {
      getJobConf().setCombinerClass( (Class<? extends org.apache.hadoop.mapred.Reducer>) c );
    } else if ( org.apache.hadoop.mapreduce.Reducer.class.isAssignableFrom( c ) ) {
      job.setCombinerClass( (Class<? extends org.apache.hadoop.mapreduce.Reducer>) c );
    }
  }

  @SuppressWarnings( "unchecked" )
  @Override
  public void setReducerClass( Class<?> c ) {
    if ( org.apache.hadoop.mapred.Reducer.class.isAssignableFrom( c ) ) {
      getJobConf().setReducerClass( (Class<? extends org.apache.hadoop.mapred.Reducer>) c );
    } else if ( org.apache.hadoop.mapreduce.Reducer.class.isAssignableFrom( c ) ) {
      job.setReducerClass( (Class<? extends org.apache.hadoop.mapreduce.Reducer>) c );
    }
  }

  @Override
  public void setOutputKeyClass( Class<?> c ) {
    job.setOutputKeyClass( c );
  }

  @Override
  public void setOutputValueClass( Class<?> c ) {
    job.setOutputValueClass( c );
  }

  @SuppressWarnings( "unchecked" )
  @Override
  public void setMapRunnerClass( Class<?> c ) {
    if ( org.apache.hadoop.mapred.MapRunnable.class.isAssignableFrom( c ) ) {
      getJobConf().setMapRunnerClass( (Class<? extends org.apache.hadoop.mapred.MapRunnable>) c );
    }
  }

  @SuppressWarnings( "unchecked" )
  @Override
  public void setInputFormat( Class<?> inputFormat ) {
    if ( org.apache.hadoop.mapred.InputFormat.class.isAssignableFrom( inputFormat ) ) {
      getJobConf().setInputFormat( (Class<? extends org.apache.hadoop.mapred.InputFormat>) inputFormat );
    } else if ( org.apache.hadoop.mapreduce.InputFormat.class.isAssignableFrom( inputFormat ) ) {
      job.setInputFormatClass( (Class<? extends org.apache.hadoop.mapreduce.InputFormat>) inputFormat );
    }
  }

  @SuppressWarnings( "unchecked" )
  @Override
  public void setOutputFormat( Class<?> outputFormat ) {
    if ( org.apache.hadoop.mapred.OutputFormat.class.isAssignableFrom( outputFormat ) ) {
      getJobConf().setOutputFormat( (Class<? extends org.apache.hadoop.mapred.OutputFormat>) outputFormat );
    } else if ( org.apache.hadoop.mapreduce.OutputFormat.class.isAssignableFrom( outputFormat ) ) {
      job.setOutputFormatClass( (Class<? extends org.apache.hadoop.mapreduce.OutputFormat>) outputFormat );
    }
  }

  @Override
  public void setInputPaths( org.pentaho.hadoop.shim.api.fs.Path... paths ) {
    if ( paths == null ) {
      return;
    }
    Path[] actualPaths = new Path[ paths.length ];
    for ( int i = 0; i < paths.length; i++ ) {
      actualPaths[ i ] = ShimUtils.asPath( paths[ i ] );
    }
    try {
      FileInputFormat.setInputPaths( job, actualPaths );
    } catch ( IOException e ) {
      e.printStackTrace();
    }
  }

  @Override
  public void setOutputPath( org.pentaho.hadoop.shim.api.fs.Path path ) {
    FileOutputFormat.setOutputPath( job, ShimUtils.asPath( path ) );
  }

  @Override
  public void setJarByClass( Class<?> c ) {
    job.setJarByClass( c );
  }

  @Override
  public void setJar( String url ) {
    job.setJar( url );
  }

  /**
   * Provide a hint to Hadoop for the number of map tasks to start for the MapReduce job submitted with this
   * configuration.
   *
   * @param n the number of map tasks for this job
   */
  @Override
  public void setNumMapTasks( int n ) {
    getJobConf().setNumMapTasks( n );
  }

  /**
   * Sets the requisite number of reduce tasks for the MapReduce job submitted with this configuration.  <p>If
   * {@code n} is {@code zero} there will not be a reduce (or sort/shuffle) phase and the output of the map tasks will
   * be written directly to the distributed file system under the path specified via {@link
   * #setOutputPath(org.pentaho.hadoop.shim.api.fs.Path)</p>
   *
   * @param n the number of reduce tasks required for this job
   * @param n
   */
  @Override
  public void setNumReduceTasks( int n ) {
    job.setNumReduceTasks( n );
  }

  /**
   * Set the array of string values for the <code>name</code> property as as comma delimited values.
   *
   * @param name   property name.
   * @param values The values
   */
  @Override
  public void setStrings( String name, String... values ) {
    getJobConf().setStrings( name, values );
  }

  /**
   * Get the default file system URL as stored in this configuration.
   *
   * @return the default URL if it was set, otherwise empty string
   */
  @Override
  public String getDefaultFileSystemURL() {
    return get( "fs.default.name", "" );
  }
}
