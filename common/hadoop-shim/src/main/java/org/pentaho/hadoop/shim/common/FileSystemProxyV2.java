/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.pentaho.hadoop.shim.common.fs.FileSystemProxy;

import java.io.IOException;

/**
 * User: Dzmitry Stsiapanau Date: 4/13/2015 Time: 06:32
 */
public class FileSystemProxyV2 extends FileSystemProxy {
  protected JobConf configuration;

  public FileSystemProxyV2( JobConf configuration ) throws IOException {
    super( FileSystem.get( configuration ) );
    this.configuration = configuration;
  }

  protected FileSystem getDelegate( org.apache.hadoop.fs.Path path ) {
    try {
      return FileSystem.get( path.toUri(), configuration );
    } catch ( IOException e ) {
      return (FileSystem) getDelegate();
    }
  }

}
