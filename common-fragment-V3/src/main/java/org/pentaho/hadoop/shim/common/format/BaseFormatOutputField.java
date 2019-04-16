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
