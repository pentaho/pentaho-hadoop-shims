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

package org.pentaho.hadoop.shim.common.format.orc;

import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.hadoop.shim.api.format.IOrcInputField;
import org.pentaho.hadoop.shim.api.format.OrcSpec.DataType;
import org.pentaho.hadoop.shim.common.format.BaseFormatInputField;

/**
 * @Author tkafalas
 */
public class OrcInputField extends BaseFormatInputField implements IOrcInputField {
  public DataType getOrcType( ) {
    return DataType.getDataType( getFormatType() );
  }

  public void setOrcType( DataType orcType ) {
    setFormatType( orcType.getId() );
  }

  public void setOrcType( String orcType ) {
    for ( DataType tmpType : DataType.values() ) {
      if ( tmpType.getName().equalsIgnoreCase( orcType ) ) {
        setFormatType( tmpType.getId() );
        break;
      }
    }
  }

  public String getTypeDesc() {
    return ValueMetaFactory.getValueMetaName( getPentahoType() );
  }

}
