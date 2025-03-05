/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.hadoop.shim.api.internal;

import org.pentaho.hadoop.shim.api.core.ShimIdentifierInterface;

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
