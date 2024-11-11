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

package org.pentaho.hadoop.shim.common.format.parquet;

import org.pentaho.hadoop.shim.api.format.IParquetOutputField;
import org.pentaho.hadoop.shim.api.format.ParquetSpec;
import org.pentaho.hadoop.shim.common.format.BaseFormatOutputField;

public class ParquetOutputField extends BaseFormatOutputField implements IParquetOutputField {
  public ParquetSpec.DataType getParquetType() {
    return ParquetSpec.DataType.values()[ formatType ];
  }

  @Override
  public void setFormatType( ParquetSpec.DataType orcType ) {
    this.formatType = orcType.ordinal();
  }

  @Override
  public void setFormatType( int formatType ) {
    for ( ParquetSpec.DataType parquetType : ParquetSpec.DataType.values() ) {
      if ( parquetType.getId() == formatType ) {
        this.formatType = formatType;
      }
    }
  }

  public void setFormatType( String typeName ) {
    try {
      setFormatType( Integer.parseInt( typeName ) );
    } catch ( NumberFormatException nfe ) {
      for ( ParquetSpec.DataType parquetType : ParquetSpec.DataType.values() ) {
        if ( parquetType.getName().equals( typeName ) ) {
          this.formatType = parquetType.ordinal();
        }
      }
    }
  }

  public boolean isDecimalType() {
    return getParquetType().getName().equals( ParquetSpec.DataType.DECIMAL.getName() );
  }
}
