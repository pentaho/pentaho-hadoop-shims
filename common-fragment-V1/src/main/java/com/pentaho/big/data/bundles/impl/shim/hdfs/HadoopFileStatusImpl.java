/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package com.pentaho.big.data.bundles.impl.shim.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileStatus;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemPath;


/**
 * Created by bryan on 5/28/15.
 */
public class HadoopFileStatusImpl implements HadoopFileStatus {
  private final FileStatus fileStatus;

  public HadoopFileStatusImpl( FileStatus fileStatus ) {
    this.fileStatus = fileStatus;
  }

  @Override
  public long getLen() {
    return fileStatus.getLen();
  }

  @Override
  public boolean isDir() {
    return fileStatus.isDir();
  }

  @Override
  public long getModificationTime() {
    return fileStatus.getModificationTime();
  }

  @Override
  public HadoopFileSystemPath getPath() {
    return new HadoopFileSystemPathImpl( fileStatus.getPath() );
  }
}
