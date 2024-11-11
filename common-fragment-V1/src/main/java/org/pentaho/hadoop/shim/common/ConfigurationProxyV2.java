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

package org.pentaho.hadoop.shim.common;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.pentaho.di.core.osgi.api.NamedClusterSiteFile;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.internal.Configuration;
import org.pentaho.hadoop.shim.api.internal.mapred.RunningJob;
import org.pentaho.hadoop.shim.ShimConfigsLoader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * User: Dzmitry Stsiapanau Date: 7/22/14 Time: 11:59 AM
 */
public class ConfigurationProxyV2 implements Configuration {

  protected Job job;

  public ConfigurationProxyV2() throws IOException {
    job = Job.getInstance();
    addConfigsForJobConf();
  }

  public ConfigurationProxyV2( String namedCluster ) throws IOException {
    job = Job.getInstance();

    // Reset static HashSets for logging
    ShimConfigsLoader.CLUSTER_NAME_FOR_LOGGING.clear();
    ShimConfigsLoader.SITE_FILE_NAME.clear();

    addConfigsForJobConf( namedCluster );
  }

  public ConfigurationProxyV2( NamedCluster namedCluster ) throws IOException {
    job = Job.getInstance();
    addConfigsFromNamedCluster( namedCluster );
  }

  private void addConfigsFromNamedCluster( NamedCluster nc ) {
    if ( nc.getSiteFiles().isEmpty() ) {
      addConfigsForJobConf();  //Backwards compatibility if there are no site files present
    } else {
      List<String> siteFileNames = Arrays.asList(
        new String[] { "hdfs-site.xml", "core-site.xml", "mapred-site.xml", "yarn-site.xml", "hbase-site.xml",
          "hive-site.xml" } );
      for ( NamedClusterSiteFile namedClusterSiteFile : nc.getSiteFiles() ) {
        if ( siteFileNames.contains( namedClusterSiteFile.getSiteFileName() ) ) {
          job.getConfiguration()
            .addResource( new ByteArrayInputStream( namedClusterSiteFile.getSiteFileContents().getBytes() ),
              namedClusterSiteFile.getSiteFileName() );
        }
      }
      ShimConfigsLoader.setSystemProperties( nc.getName() );
    }
  }

  @VisibleForTesting
  void addConfigsForJobConf() {
    job.getConfiguration().addResource( "hdfs-site.xml" );
    job.getConfiguration().addResource( "core-site.xml" );
    job.getConfiguration().addResource( "mapred-site.xml" );
    job.getConfiguration().addResource( "yarn-site.xml" );
    job.getConfiguration().addResource( "hbase-site.xml" );
    job.getConfiguration().addResource( "hive-site.xml" );
  }

  @VisibleForTesting
  void addConfigsForJobConf( String additionalPath ) {
    ShimConfigsLoader.addConfigsAsResources( additionalPath, getJob().getConfiguration()::addResource,
      ShimConfigsLoader.ClusterConfigNames.CORE_SITE,
      ShimConfigsLoader.ClusterConfigNames.MAPRED_SITE,
      ShimConfigsLoader.ClusterConfigNames.HDFS_SITE,
      ShimConfigsLoader.ClusterConfigNames.YARN_SITE,
      ShimConfigsLoader.ClusterConfigNames.HBASE_SITE,
      ShimConfigsLoader.ClusterConfigNames.HIVE_SITE );
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
    getJob().setJobName( jobName );
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
    getJob().setMapOutputKeyClass( c );
  }

  /**
   * Set the value class for the map output data.
   *
   * @param c the map output value class
   */
  @Override
  public void setMapOutputValueClass( Class<?> c ) {
    getJob().setMapOutputValueClass( c );
  }

  @SuppressWarnings( "unchecked" )
  @Override
  public void setMapperClass( Class<?> c ) {
    if ( org.apache.hadoop.mapred.Mapper.class.isAssignableFrom( c ) ) {
      setUseOldMapApi();
      getJobConf().setMapperClass( (Class<? extends org.apache.hadoop.mapred.Mapper>) c );
    } else if ( org.apache.hadoop.mapreduce.Mapper.class.isAssignableFrom( c ) ) {
      getJob().setMapperClass( (Class<? extends org.apache.hadoop.mapreduce.Mapper>) c );
    }
  }

  private void setUseOldMapApi() {
    set( "mapred.mapper.new-api", "false" );
  }

  @SuppressWarnings( "unchecked" )
  @Override
  public void setCombinerClass( Class<?> c ) {
    if ( org.apache.hadoop.mapred.Reducer.class.isAssignableFrom( c ) ) {
      setUseOldRedApi();
      getJobConf().setCombinerClass( (Class<? extends org.apache.hadoop.mapred.Reducer>) c );
    } else if ( org.apache.hadoop.mapreduce.Reducer.class.isAssignableFrom( c ) ) {
      getJob().setCombinerClass( (Class<? extends org.apache.hadoop.mapreduce.Reducer>) c );
    }
  }

  private void setUseOldRedApi() {
    set( "mapred.reducer.new-api", "false" );
  }

  @SuppressWarnings( "unchecked" )
  @Override
  public void setReducerClass( Class<?> c ) {
    if ( org.apache.hadoop.mapred.Reducer.class.isAssignableFrom( c ) ) {
      setUseOldRedApi();
      getJobConf().setReducerClass( (Class<? extends org.apache.hadoop.mapred.Reducer>) c );
    } else if ( org.apache.hadoop.mapreduce.Reducer.class.isAssignableFrom( c ) ) {
      getJob().setReducerClass( (Class<? extends org.apache.hadoop.mapreduce.Reducer>) c );
    }
  }

  @Override
  public void setOutputKeyClass( Class<?> c ) {
    getJob().setOutputKeyClass( c );
  }

  @Override
  public void setOutputValueClass( Class<?> c ) {
    getJob().setOutputValueClass( c );
  }

  @SuppressWarnings( "unchecked" )
  @Override
  public void setMapRunnerClass( String className ) {
    getJobConf().set( "mapred.map.runner.class", className );
  }

  @SuppressWarnings( "unchecked" )
  @Override
  public void setInputFormat( Class<?> inputFormat ) {
    if ( org.apache.hadoop.mapred.InputFormat.class.isAssignableFrom( inputFormat ) ) {
      setUseOldMapApi();
      getJobConf().setInputFormat( (Class<? extends org.apache.hadoop.mapred.InputFormat>) inputFormat );
    } else if ( org.apache.hadoop.mapreduce.InputFormat.class.isAssignableFrom( inputFormat ) ) {
      getJob().setInputFormatClass( (Class<? extends org.apache.hadoop.mapreduce.InputFormat>) inputFormat );
    }
  }

  @SuppressWarnings( "unchecked" )
  @Override
  public void setOutputFormat( Class<?> outputFormat ) {
    if ( org.apache.hadoop.mapred.OutputFormat.class.isAssignableFrom( outputFormat ) ) {
      setUseOldRedApi();
      if ( getJobConf().getNumReduceTasks() == 0 || get( "mapred.partitioner.class" ) != null ) {
        setUseOldMapApi();
      }
      getJobConf().setOutputFormat( (Class<? extends org.apache.hadoop.mapred.OutputFormat>) outputFormat );
    } else if ( org.apache.hadoop.mapreduce.OutputFormat.class.isAssignableFrom( outputFormat ) ) {
      getJob().setOutputFormatClass( (Class<? extends org.apache.hadoop.mapreduce.OutputFormat>) outputFormat );
    }
  }

  @Override
  public void setInputPaths( org.pentaho.hadoop.shim.api.internal.fs.Path... paths ) {
    if ( paths == null ) {
      return;
    }
    Path[] actualPaths = new Path[ paths.length ];
    for ( int i = 0; i < paths.length; i++ ) {
      actualPaths[ i ] = ShimUtils.asPath( paths[ i ] );
    }
    try {
      FileInputFormat.setInputPaths( getJob(), actualPaths );
    } catch ( IOException e ) {
      e.printStackTrace();
    }
  }

  @Override
  public void setOutputPath( org.pentaho.hadoop.shim.api.internal.fs.Path path ) {
    FileOutputFormat.setOutputPath( getJob(), ShimUtils.asPath( path ) );
  }

  @Override
  public void setJarByClass( Class<?> c ) {
    getJob().setJarByClass( c );
  }

  @Override
  public void setJar( String url ) {
    getJob().setJar( url );
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
   * Sets the requisite number of reduce tasks for the MapReduce job submitted with this configuration.  <p>If {@code n}
   * is {@code zero} there will not be a reduce (or sort/shuffle) phase and the output of the map tasks will be written
   * directly to the distributed file system under the path specified via org.pentaho.hadoop
   * .shim.api.fs.Path.setOutputPath()
   *
   * @param n the number of reduce tasks required for this job
   * @param n
   */
  @Override
  public void setNumReduceTasks( int n ) {
    getJob().setNumReduceTasks( n );
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

  /**
   * Hack Return this configuration as was asked with provided delegate class (If it is possible).
   *
   * @param delegate class of desired return object
   * @return this configuration delegate object if possible
   */
  @Override
  public <T> T getAsDelegateConf( Class<T> delegate ) {
    if ( delegate.isAssignableFrom( this.getClass() ) ) {
      return (T) this;
    } else if ( delegate.isAssignableFrom( JobConf.class ) ) {
      return (T) getJobConf();
    } else {
      return null;
    }
  }

  /**
   * Submit job for the current configuration provided by this implementation.
   *
   * @return RunningJob implementation
   */
  @Override public RunningJob submit() throws IOException, ClassNotFoundException, InterruptedException {
    if ( YarnQueueAclsVerifier
      .verify( ( createClusterDescription( getJob().getConfiguration() ) ).getQueueAclsForCurrentUser() ) ) {
      getJob().submit();
      return new RunningJobProxyV2( getJob() );
    } else {
      throw new YarnQueueAclsException( BaseMessages.getString( ConfigurationProxy.class,
        "ConfigurationProxy.UserHasNoPermissions", UserGroupInformation.getCurrentUser().getUserName() ) );
    }
  }

  Cluster createClusterDescription( org.apache.hadoop.conf.Configuration configuration ) throws IOException {
    return new Cluster( configuration );
  }
}
