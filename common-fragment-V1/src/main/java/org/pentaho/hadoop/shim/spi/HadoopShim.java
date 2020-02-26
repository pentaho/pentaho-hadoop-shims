/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package org.pentaho.hadoop.shim.spi;

import java.io.IOException;
import java.sql.Driver;
import java.util.List;

import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.ConfigurationException;
import org.pentaho.hadoop.shim.api.internal.Configuration;
import org.pentaho.hadoop.shim.api.internal.DistributedCacheUtil;
import org.pentaho.hadoop.shim.api.internal.Required;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.internal.fs.FileSystem;
import org.pentaho.hadoop.shim.api.internal.mapred.RunningJob;

import java.net.URI;

/**
 * Abstracts a Hadoop environment so that it may be swapped out at runtime.
 *
 * @author Jordan Ganoff (jganoff@pentaho.com)
 */
@Required
public interface HadoopShim {

  /**
   * Retrieve a JDBC driver capable of querying Hive for the version of Hadoop this shim abstracts.
   *
   * @return a valid Hive JDBC driver
   * @deprecated Replaced by {@link #getJdbcDriver(String driverType)}
   */
  @Deprecated Driver getHiveJdbcDriver();

  /**
   * Retrieve a JDBC driver for the specified service for the version of Hadoop this shim abstracts.
   *
   * @return a valid JDBC driver
   */
  Driver getJdbcDriver( String driverType );


  /**
   * Get the namenode connection information, if applicable, for the Hadoop configuration.
   *
   * @param c Configuration to retrieve namenode connection information from.
   * @return Tuple of {hostname, port} of the namenode if the connection information could be determined.
   */
  String[] getNamenodeConnectionInfo( Configuration c );

  /**
   * Get the jobtracker connection information, if applicable, for the Hadoop configuration.
   *
   * @param c Configuration to retrieve jobtracker connection information from.
   * @return Tuple of {hostname, port} of the jobtracker if the connection information could be determined.
   */
  String[] getJobtrackerConnectionInfo( Configuration c );

  /**
   * Retrieve the version of Hadoop this shim wraps.
   *
   * @return The Hadoop version that is abstracted behind this shim implementation.
   */
  String getHadoopVersion();

  /**
   * Creates a Configuration with default properties loaded from the Hadoop configuration.
   *
   * @return Configuration with default properties loaded and any additional properties set specific to this Hadoop
   * configuration.
   */
  Configuration createConfiguration();

  /**
   * @deprecated Use the {@link #createConfiguration(NamedCluster)} method
   * @param namedCluster
   * @return
   */
  @Deprecated
  Configuration createConfiguration( String namedCluster );

  /**
   * Create the configuration from the sites files in the NamedCluster
   *
   * @param namedCluster
   * @return
   */
  Configuration createConfiguration( NamedCluster namedCluster );

  /**
   * Look up a file system abstraction using the configuration provided
   *
   * @param conf Configuration properties
   * @return A File system abstraction configured with the properties found in {@code conf}
   * @throws IOException Error looking up/creating the file system
   */
  FileSystem getFileSystem( Configuration conf ) throws IOException;

  default FileSystem getFileSystem( URI uri, Configuration conf, NamedCluster namedCluster )
    throws IOException, InterruptedException {
    return getFileSystem( uri, conf, (String) null );
  }

  /**
   * Look up a file system abstraction using the configuration provided
   *
   * @param uri  Uri of filesystem
   * @param conf Configuration properties
   * @param user User to run. Null -> currentUser()
   * @return A File system abstraction configured with the properties found in {@code conf}
   * @throws IOException Error looking up/creating the file system
   */
  default FileSystem getFileSystem( URI uri, Configuration conf, String user )
    throws IOException, InterruptedException {
    return getFileSystem( conf );
  }

  /**
   * Setup the config object based on the supplied information with respect to the specific distribution
   *
   * @param namenodeHost
   * @param namenodePort
   * @param jobtrackerHost
   * @param jobtrackerPort
   * @param conf           Configuration to update
   * @param logMessages    Any basic log messages that should be logged
   * @throws Exception Error configuring with the provided connection information
   */
  void configureConnectionInformation( String namenodeHost, String namenodePort, String jobtrackerHost,
                                       String jobtrackerPort, Configuration conf, List<String> logMessages )
    throws Exception;

  /**
   * Get the utility for manipulating files stored in the Distributed Cache.
   *
   * @return the distributed cache utility
   * @throws ConfigurationException Error retrieving the distributed cache util
   */
  DistributedCacheUtil getDistributedCacheUtil() throws ConfigurationException;

  /**
   * Submit a MapReduce job based off the provided configuration.
   *
   * @param c Configuration for the MapReduce job
   * @return A handle to the running job which can be used to track progress
   */
  RunningJob submitJob( Configuration c ) throws IOException;

  /**
   * Returns the dependency classes nedeed by Sqoop to perform HBase operations as dictated in the
   * TableMapReduceUtil.addHBaseDependencyJars() method in the HBase distribution used by the shim.
   *
   * @retuen A Class array containing the dependency classes needed
   */
  Class[] getHbaseDependencyClasses();

  /**
   * Determine the Hadoop writable type to pass Kettle type back to Hadoop as.
   *
   * @param kettleType Value meta to look up compatible Hadoop Writable class.
   * @return Java type to convert {@code kettleType} to when sending data back to Hadoop.
   * @deprecated To be replaced with a cleaner API for executing Pentaho MapReduce. Use with care.
   */
  @Deprecated Class<?> getHadoopWritableCompatibleClass( ValueMetaInterface kettleType );

  /**
   * Get the {@link Class} to use for the Combiner in a Pentaho MapReduce job.
   *
   * @return the class to use for the Combiner in a Pentaho MapReduce job
   * @deprecated To be replaced with a cleaner API for executing Pentaho MapReduce. Use with care.
   */
  @Deprecated String getPentahoMapReduceCombinerClass();

  /**
   * Get the {@link Class} to use for the Reducer in a Pentaho MapReduce job.
   *
   * @return the class to use for the Reducer in a Pentaho MapReduce job
   * @deprecated To be replaced with a cleaner API for executing Pentaho MapReduce. Use with care.
   */
  @Deprecated String getPentahoMapReduceReducerClass();

  /**
   * Get the {@link Class} to use for the MapRunner in a Pentaho MapReduce job.
   *
   * @return the class to use for the MapRunner in a Pentaho MapReduce job
   * @deprecated To be replaced with a cleaner API for executing Pentaho MapReduce. Use with care.
   */
  @Deprecated String getPentahoMapReduceMapRunnerClass();
}
