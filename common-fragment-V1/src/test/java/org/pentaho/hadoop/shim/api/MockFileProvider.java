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

package org.pentaho.hadoop.shim.api;

import java.util.Collection;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.FileProvider;

public class MockFileProvider implements FileProvider {

  @Override
  public FileObject createFileSystem( String arg0, FileObject arg1, FileSystemOptions arg2 )
    throws FileSystemException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public FileObject findFile( FileObject arg0, String arg1, FileSystemOptions arg2 ) throws FileSystemException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Collection getCapabilities() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public FileSystemConfigBuilder getConfigBuilder() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public FileName parseUri( FileName arg0, String arg1 ) throws FileSystemException {
    // TODO Auto-generated method stub
    return null;
  }

}
