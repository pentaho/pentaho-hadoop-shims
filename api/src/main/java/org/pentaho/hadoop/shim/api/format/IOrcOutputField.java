package org.pentaho.hadoop.shim.api.format;

public interface IOrcOutputField extends IFormatOutputField {
  OrcSpec.DataType getOrcType();

  void setFormatType( OrcSpec.DataType type );
}
