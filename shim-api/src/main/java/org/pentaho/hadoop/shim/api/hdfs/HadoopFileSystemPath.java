/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package org.pentaho.hadoop.shim.api.hdfs;

import java.net.URI;

/**
 * Created by bryan on 5/27/15.
 */
public interface HadoopFileSystemPath {
  String getPath();

  String getName();

  String toString();

  URI toUri();

  HadoopFileSystemPath resolve( HadoopFileSystemPath child );

  HadoopFileSystemPath resolve( String child );

  HadoopFileSystemPath getParent();
}
