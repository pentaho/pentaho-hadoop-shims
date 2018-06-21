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

package org.pentaho.bigdata.api.hdfs;

import org.pentaho.big.data.api.cluster.NamedCluster;

import java.io.IOException;
import java.net.URI;

/**
 * Created by bryan on 5/28/15.
 */
public interface HadoopFileSystemFactory {

  boolean canHandle( NamedCluster namedCluster );

  @Deprecated
  HadoopFileSystem create( NamedCluster namedCluster ) throws IOException;

  HadoopFileSystem create( NamedCluster namedCluster, URI uri ) throws IOException;
}
