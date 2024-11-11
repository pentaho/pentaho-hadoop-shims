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

public interface IOrcOutputField extends IFormatOutputField {
  OrcSpec.DataType getOrcType();

  void setFormatType( OrcSpec.DataType type );
}
