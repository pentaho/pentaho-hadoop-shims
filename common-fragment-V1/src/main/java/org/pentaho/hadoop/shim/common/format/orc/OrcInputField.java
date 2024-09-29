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


package org.pentaho.hadoop.shim.common.format.orc;

import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.hadoop.shim.api.format.IOrcInputField;
import org.pentaho.hadoop.shim.api.format.OrcSpec.DataType;
import org.pentaho.hadoop.shim.common.format.BaseFormatInputField;

/**
 * @Author tkafalas
 */
public class OrcInputField extends BaseFormatInputField implements IOrcInputField {
  public DataType getOrcType() {
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
