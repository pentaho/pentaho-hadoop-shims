/*******************************************************************************
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

package org.pentaho.hadoop.shim.mapr31;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.common.DistributedCacheUtilImpl;

public class MapR3DistributedCacheUtilImpl extends DistributedCacheUtilImpl {

  public MapR3DistributedCacheUtilImpl( HadoopConfiguration configuration ) {
    super( configuration );
  }

  /**
   * Add an file path to the current set of classpath entries. It adds the file
   * to cache as well.
   *
   * This is copied from Hadoop 0.20.2 o.a.h.filecache.DistributedCache so we can inject the correct path separator
   * for the environment the cluster is executing in. See {@link #getClusterPathSeparator()}.
   *
   * @param file Path of the file to be added
   * @param conf Configuration that contains the classpath setting
   */
  @Override
  public void addFileToClassPath(Path file, Configuration conf)
      throws IOException {
    
    String classpath = conf.get("mapred.job.classpath.files");
    conf.set("mapred.job.classpath.files", classpath == null ? file.toString()
        : classpath + getClusterPathSeparator() + file.toString());
    FileSystem fs = FileSystem.get(conf);
    URI uri = fs.makeQualified(file).toUri();

    DistributedCache.addCacheFile(uri, conf);
  }
}
