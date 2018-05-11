/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.api;

import org.pentaho.hadoop.shim.spi.ShimIdentifierInterface;

public class ShimIdentifier implements ShimIdentifierInterface {
  private String id;

  private String vendor;

  private String version;

  private ShimType type;

  public ShimIdentifier( String id, String vendor, String version, ShimType type ) {
    this.setId( id );
    this.setVendor( vendor );
    this.setVersion( version );
    this.setType( type );
  }

  public String getId() {
    return this.id;
  }

  public void setId( String id ) {
    this.id = id;
  }

  public String getVendor() {
    return vendor;
  }

  public void setVendor( String vendor ) {
    this.vendor = vendor;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion( String version ) {
    this.version = version;
  }

  public ShimType getType() {
    return type;
  }

  public void setType( ShimType type ) {
    this.type = type;
  }
}
