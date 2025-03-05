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


package org.pentaho.hadoop.shim.common.fs;

import org.pentaho.hadoop.shim.api.internal.fs.Path;

public class PathProxy extends org.apache.hadoop.fs.Path implements Path {

  public PathProxy( String path ) {
    super( path );
  }

  public PathProxy( String parent, String child ) {
    this( new PathProxy( parent ), child );
  }

  public PathProxy( Path parent, String child ) {
    super( (org.apache.hadoop.fs.Path) parent, child );
  }
}
