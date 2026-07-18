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
