/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.common;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.security.UserGroupInformation;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hadoop.mapreduce.YarnQueueAclsException;
import org.pentaho.hadoop.mapreduce.YarnQueueAclsVerifier;
import org.pentaho.hadoop.shim.api.mapred.RunningJob;
import org.pentaho.hadoop.shim.common.mapred.RunningJobProxy;

import java.io.IOException;

/**
 * A common configuration object representing org.apache.hadoop.conf.Configuration. <p> This has been un-deprecated in
 * future version of Hadoop and thus the deprecation warning can be safely ignored. </p>
 */
@SuppressWarnings( { "unchecked", "rawtypes" } )
public class ConfigurationProxy extends org.apache.hadoop.mapred.JobConf implements
  org.pentaho.hadoop.shim.api.Configuration {

  public ConfigurationProxy() {
    super();
    addResource( "hdfs-site.xml" );
  }
  /*
   * Wrap the call to {@link super#setMapperClass(Class)} to avoid generic type
   * mismatches. We do not expose {@link org.apache.hadoop.mapred.*} classes through
   * the API or provide proxies for them. This pattern is used for many of the
   * class setter methods in this implementation.
   */

  @Override
  public void setMapperClass( Class c ) {
    super.setMapperClass( (Class<? extends Mapper>) c );
  }

  @Override
  public void setCombinerClass( Class c ) {
    super.setCombinerClass( (Class<? extends Reducer>) c );
  }

  @Override
  public void setReducerClass( Class c ) {
    super.setReducerClass( (Class<? extends Reducer>) c );
  }

  @Override
  public void setMapRunnerClass( Class c ) {
    super.setMapRunnerClass( (Class<? extends MapRunnable>) c );
  }

  @Override
  public void setInputFormat( Class c ) {
    super.setInputFormat( (Class<? extends InputFormat>) c );
  }

  @Override
  public void setOutputFormat( Class c ) {
    super.setOutputFormat( (Class<? extends OutputFormat>) c );
  }

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
    JobClient jobClient = createJobClient();
    if ( YarnQueueAclsVerifier.verify( jobClient.getQueueAclsForCurrentUser() ) ) {
      return new RunningJobProxy( jobClient.submitJob( this ) );
    } else {
      throw new YarnQueueAclsException( BaseMessages.getString( ConfigurationProxy.class,
        "ConfigurationProxy.UserHasNoPermissions", UserGroupInformation.getCurrentUser().getUserName() ) );
    }
  }

  JobClient createJobClient() throws IOException {
    return new JobClient( this );
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
    FileInputFormat.setInputPaths( this, actualPaths );
  }

  @Override
  public void setOutputPath( org.pentaho.hadoop.shim.api.fs.Path path ) {
    FileOutputFormat.setOutputPath( this, ShimUtils.asPath( path ) );
  }
}
