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


package org.pentaho.hadoop.shim.api.internal.fs;

import java.io.IOException;

/**
 * An abstraction for {@link org.apache.hadoop.fs.FileSystem}.
 *
 * @author Jordan Ganoff (jganoff@pentaho.com)
 */
public interface FileSystem {
  /**
   * @return the underlying File System implementation
   */
  Object getDelegate();

  /**
   * Create a {@link Path} object out of the path string provided.
   *
   * @param path Location of path to create
   * @return Path to the string provided
   */
  Path asPath( String path );

  /**
   * Creates a path by composing a parent and a relative path to a child.
   *
   * @param parent Parent path
   * @param child  String representing the location of the child path relative to {@code parent}
   * @return Path of child relative to parent
   */
  Path asPath( Path parent, String child );

  /**
   * @param parent String representing the location of the path to use to resolve {@code child}
   * @param child  String representing the location of the child path relative to {@code parent}
   * @return Path of child relative to parent
   * @see #asPath(Path, String)
   */
  Path asPath( String parent, String child );

  /**
   * Does the path reference an object?
   *
   * @param path Path
   * @return {@code true} if the path points to an object
   * @throws IOException Error communicating with the file system
   */
  boolean exists( Path path ) throws IOException;

  /**
   * Removes the path provided.
   *
   * @param path      Path to remove
   * @param recursive Flag indicating if we should delete all children (recursively)
   * @return {@code true} if the path was deleted successfully
   * @throws IOException Error deleting path
   */
  boolean delete( Path path, boolean recursive ) throws IOException;
}
