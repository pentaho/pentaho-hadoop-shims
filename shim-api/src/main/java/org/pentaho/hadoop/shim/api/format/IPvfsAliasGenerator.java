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

package org.pentaho.hadoop.shim.api.format;

public interface IPvfsAliasGenerator {
  /**
   * This method should be called on any url before using the  Hadoop FileSystem api.  If a non-null url is returned
   * then that url should be used to process a temporary staging file.  VFS should then be used to copy to this
   * temporary file to its final destination.
   * @param pvfsPath The uri of the file to be operated on.
   * @return The uri to use instead, or null if the path is ok as is.
   */
  String generateAlias( String pvfsPath );
}
