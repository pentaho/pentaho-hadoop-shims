/*! ******************************************************************************
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
package org.pentaho.hadoop.shim.mapr510;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * User: Dzmitry Stsiapanau Date: 7/22/14 Time: 11:59 AM
 */
public class ConfigurationProxyV2 extends org.pentaho.hadoop.shim.common.ConfigurationProxyV2 {

  protected class JobProxy extends Job {
    private JobProxy( JobConf conf ) throws IOException {
      super( conf );
    }

    void refreshUGI() {
      try {
        this.ugi = UserGroupInformation.getCurrentUser();
      } catch ( IOException e ) {
        throw new RuntimeException( e );
      }
    }
  }

  public ConfigurationProxyV2() throws IOException {
    // create with a null Cluster
    JobConf jobConf = new JobConf( new org.apache.hadoop.conf.Configuration() );
    job = new JobProxy( jobConf );
    job.getConfiguration().addResource( "hdfs-site.xml" );
  }

  public JobConf getJobConf() {
    return (JobConf) getJob().getConfiguration();
  }

  public Job getJob() {
    ( (JobProxy) job ).refreshUGI();
    return job;
  }

}
