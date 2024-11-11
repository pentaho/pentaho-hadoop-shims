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

import org.pentaho.hadoop.shim.api.internal.Configuration;
import org.pentaho.hadoop.shim.api.internal.fs.FileSystem;
import org.pentaho.hadoop.shim.api.internal.fs.Path;

public class ShimUtils {

  public static org.apache.hadoop.fs.FileSystem asFileSystem( FileSystem fs ) {
    return fs == null ? null : (org.apache.hadoop.fs.FileSystem) fs.getDelegate();
  }

  @SuppressWarnings( "deprecation" )
  public static org.apache.hadoop.mapred.JobConf asConfiguration( Configuration c ) {
    return c.getAsDelegateConf( org.apache.hadoop.mapred.JobConf.class );
  }

  public static org.apache.hadoop.fs.Path asPath( Path path ) {
    return (org.apache.hadoop.fs.Path) path;
  }
}
