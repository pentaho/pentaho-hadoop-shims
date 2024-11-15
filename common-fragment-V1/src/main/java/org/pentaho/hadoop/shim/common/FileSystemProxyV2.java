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
    super( get( configuration ) );
    this.configuration = configuration;
  }

  protected FileSystem getDelegate( org.apache.hadoop.fs.Path path ) {
    try {
      return get( path.toUri(), configuration );
    } catch ( IOException e ) {
      return (FileSystem) getDelegate();
    }
  }

}
