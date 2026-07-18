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

/**
 * Created by bryan on 5/27/15.
 */
public interface HadoopFileStatus {
  long getLen();

  boolean isDir();

  long getModificationTime();

  HadoopFileSystemPath getPath();
}
