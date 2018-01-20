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

package org.pentaho.hadoop.shim.api.format;

public interface IAvroInputField {
  public static final String FILENAME_DELIMITER = "_delimiter_";

  public String getAvroFieldName();

  public void setAvroFieldName( String avroFieldName );

  public String getPentahoFieldName();

  public void setPentahoFieldName( String pentahoFieldName );

  public int getPentahoType();

  public void setPentahoType( int pentahoType );

  public AvroSpec.DataType getAvroType();

  public void setAvroType( AvroSpec.DataType avroType );

  public void setAvroType( String avroType );

  public String getDisplayableAvroFieldName();
}
