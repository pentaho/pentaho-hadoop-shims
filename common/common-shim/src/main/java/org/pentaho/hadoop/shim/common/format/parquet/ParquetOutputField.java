/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
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
    try  {
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
