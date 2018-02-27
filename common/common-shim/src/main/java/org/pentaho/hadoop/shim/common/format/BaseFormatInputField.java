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
  public void setPentahoType( String value ) {
    setPentahoType( ValueMetaFactory.getIdForValueMeta( value ) );
  }
}
