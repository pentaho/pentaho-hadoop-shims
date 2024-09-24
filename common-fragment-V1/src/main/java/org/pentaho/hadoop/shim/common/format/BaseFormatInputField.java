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

import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.hadoop.shim.api.format.IFormatInputField;

public class BaseFormatInputField implements IFormatInputField {
  protected String formatFieldName = null;
  private String pentahoFieldName = null;
  private int pentahoType;
  private int formatType;
  private int scale = 0;
  private int precision = 0;
  private String stringFormat = "";

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
  public int getPrecision() {
    return precision;
  }

  @Override
  public void setPrecision( int precision ) {
    this.precision = precision;
  }

  @Override
  public int getScale() {
    return scale;
  }

  @Override
  public void setScale( int scale ) {
    this.scale = scale;
  }

  @Override
  public int getPentahoType() {
    return pentahoType;
  }

  @Override
  public void setPentahoType( int pentahoType ) {
    this.pentahoType = pentahoType;
  }

  @Override public int getFormatType() {
    return formatType;
  }

  @Override public void setFormatType( int formatType ) {
    this.formatType = formatType;
  }

  @Override
  public String getStringFormat() {
    return stringFormat;
  }

  @Override
  public void setStringFormat( String stringFormat ) {
    this.stringFormat = stringFormat == null ? "" : stringFormat;
  }

  @Override
  public void setPentahoType( String value ) {
    setPentahoType( ValueMetaFactory.getIdForValueMeta( value ) );
  }
}
