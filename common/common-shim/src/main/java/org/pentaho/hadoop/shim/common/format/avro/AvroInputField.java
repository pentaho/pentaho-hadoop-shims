package org.pentaho.hadoop.shim.common.format.avro;

import org.pentaho.hadoop.shim.api.format.AvroSpec;
import org.pentaho.hadoop.shim.api.format.IAvroInputField;

public class AvroInputField implements IAvroInputField {
  protected String avroFieldName = null;
  private String pentahoFieldName = null;
  private int pentahoType;
  private AvroSpec.DataType avroType = null;

  @Override
  public String getAvroFieldName() {
    return avroFieldName;
  }

  @Override
  public void setAvroFieldName( String avroFieldName ) {
    this.avroFieldName = avroFieldName;
  }

  @Override
  public String getPentahoFieldName() {
    return pentahoFieldName;
  }

  @Override
  public void setPentahoFieldName( String pentahoFieldName ) {
    this.pentahoFieldName = pentahoFieldName;
  }

  @Override
  public int getPentahoType() {
    return pentahoType;
  }

  @Override
  public void setPentahoType( int pentahoType ) {
    this.pentahoType = pentahoType;
  }

  @Override
  public AvroSpec.DataType getAvroType() {
    return avroType;
  }

  @Override
  public void setAvroType( AvroSpec.DataType avroType ) {
    this.avroType = avroType;
  }

  @Override
  public void setAvroType( String avroType ) {
    for ( AvroSpec.DataType tmpType : AvroSpec.DataType.values() ) {
      if ( tmpType.getName().equalsIgnoreCase( avroType ) ) {
        this.avroType = tmpType;
        break;
      }
    }
  }

  @Override
  public String getDisplayableAvroFieldName() {
    String displayableAvroFieldName = avroFieldName;
    if ( avroFieldName.contains( FILENAME_DELIMITER ) ) {
      displayableAvroFieldName = avroFieldName.split( FILENAME_DELIMITER )[0];
    }

    return displayableAvroFieldName;
  }
}
