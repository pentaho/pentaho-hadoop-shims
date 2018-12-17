/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.api;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.format.FormatService;
import org.pentaho.hadoop.shim.api.hbase.HBaseService;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystem;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceService;
import org.pentaho.hadoop.shim.api.oozie.OozieService;
import org.pentaho.hadoop.shim.api.pig.PigService;
import org.pentaho.hadoop.shim.api.sqoop.SqoopService;

import java.io.IOException;
import java.net.URI;

public interface ShimServicesFactoryInterface {
  FormatService createFormatService(NamedCluster namedCluster);

  SqoopService createSqoopService(NamedCluster namedCluster);

  PigService createPigService(NamedCluster namedCluster);

  MapReduceService createMapReduceService(NamedCluster namedCluster);

  OozieService createOozieService( NamedCluster namedCluster );

  HBaseService createHBaseService( NamedCluster namedCluster ) throws Exception;

  HadoopFileSystem createHadoopFileSystem( NamedCluster namedCluster ) throws IOException;

  HadoopFileSystem createHadoopFileSystem( NamedCluster namedCluster, URI uri ) throws IOException;
}
