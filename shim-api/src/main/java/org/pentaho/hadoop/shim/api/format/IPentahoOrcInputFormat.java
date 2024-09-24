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

public interface IPentahoOrcInputFormat extends IPentahoInputFormat {
  /**
   * Read schema for display to user.
   */
  List<IOrcInputField> readSchema();

  /**
   * Set schema for file reading.
   */
  void setSchema( List<IOrcInputField> orcInputField );

  /**
   * Set input file.
   */
  void setInputFile( String file );

}
