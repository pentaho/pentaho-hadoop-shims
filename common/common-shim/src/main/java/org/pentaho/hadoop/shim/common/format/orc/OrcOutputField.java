package org.pentaho.hadoop.shim.common.format.orc;

import org.pentaho.hadoop.shim.api.format.IOrcOutputField;
import org.pentaho.hadoop.shim.api.format.OrcSpec;
import org.pentaho.hadoop.shim.common.format.BaseFormatOutputField;

public class OrcOutputField extends BaseFormatOutputField implements IOrcOutputField {
  public OrcSpec.DataType getOrcType() {
    return OrcSpec.DataType.values()[ formatType ];
  }

  @Override
  public void setFormatType( OrcSpec.DataType orcType ) {
    this.formatType = orcType.ordinal();
  }

  @Override
  public void setFormatType( int formatType ) {
    for ( OrcSpec.DataType orcType : OrcSpec.DataType.values() ) {
      if ( orcType.ordinal() == formatType ) {
        this.formatType = formatType;
      }
    }
  }

  public void setFormatType( String typeName ) {
    try  {
      setFormatType( Integer.parseInt( typeName ) );
    } catch ( NumberFormatException nfe ) {
      for ( OrcSpec.DataType orcType : OrcSpec.DataType.values() ) {
        if ( orcType.getName().equals( typeName ) ) {
          this.formatType = orcType.ordinal();
        }
      }
    }
  }

  public boolean isDecimalType() {
    return getOrcType().getName().equals( OrcSpec.DataType.DECIMAL.getName() );
  }
}
