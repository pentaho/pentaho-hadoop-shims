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

package org.pentaho.hadoop.shim.common.format;

import org.pentaho.hadoop.shim.api.format.IFormatOutputField;

public class BaseFormatOutputField implements IFormatOutputField {
  protected String formatFieldName;
  protected String pentahoFieldName;
  protected int pentahoType;
  protected boolean allowNull;
  protected String defaultValue;
  protected int formatType;
  protected int precision;
  protected int scale;

  @Override
  public String getFormatFieldName() {
    return formatFieldName;
  }

  @Override
  public void setFormatFieldName( String formatFieldName ) {
    this.formatFieldName = formatFieldName;
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
  public boolean getAllowNull() {
    return allowNull;
  }

  @Override
  public void setAllowNull( boolean allowNull ) {
    this.allowNull = allowNull;
  }

  @Override
  public String getDefaultValue() {
    return defaultValue;
  }

  @Override
  public void setDefaultValue( String defaultValue ) {
    this.defaultValue = defaultValue;
  }

  @Override
  public void setFormatType( int formatType ) {
    this.formatType = formatType;
  }

  @Override
  public int getPrecision() {
    return precision;
  }

  @Override
  public void setPrecision( String precision ) {
    this.precision = Integer.valueOf( precision );
  }

  @Override
  public int getScale() {
    return scale;
  }

  public void setScale( String scale ) {
    this.scale = Integer.valueOf( scale );
  }

  @Override
  public int getFormatType() {
    return formatType;
  }

  @Override
  public int getPentahoType() {
    return pentahoType;
  }

  @Override
  public void setPentahoType( int pentahoType ) {
    this.pentahoType = pentahoType;
  }
}
