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

public interface IParquetOutputField extends IFormatOutputField {
  ParquetSpec.DataType getParquetType();

  void setFormatType( ParquetSpec.DataType type );
}
