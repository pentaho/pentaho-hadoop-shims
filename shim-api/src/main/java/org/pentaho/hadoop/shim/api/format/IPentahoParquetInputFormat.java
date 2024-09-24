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
package org.pentaho.hadoop.shim.api.format;

import java.util.List;

@SuppressWarnings( { "squid:S1452", "squid:S112" } )
public interface IPentahoParquetInputFormat extends IPentahoInputFormat {
  /**
   * Read schema for display to user.
   */
  List<? extends IParquetInputField> readSchema( String file ) throws Exception;

  /**
   * Set schema for file reading.
   */
  void setSchema( List<IParquetInputField> inputFields ) throws Exception;

  /**
   * Set input file.
   */
  void setInputFile( String file ) throws Exception;

  /**
   * Set input files.
   */
  void setInputFiles( String[] files ) throws Exception;

  /**
   * Split size, bytes.
   */
  void setSplitSize( long blockSize ) throws Exception;
}
