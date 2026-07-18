/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/


package org.pentaho.big.data.api.shims;

import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;

@MetaStoreElementType( name = "defaultShim", description = "The default vendor shim" )
public class DefaultShim {
  @MetaStoreAttribute
  private String defaultShim;
  private String name = "DefaultShim";

  //No Argument constructor required by MetastoreFactory
  public DefaultShim() {

  }

  public DefaultShim( String defaultShim ) {
    this.defaultShim = defaultShim;
  }

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getDefaultShim() {
    return defaultShim;
  }

  public void setDefaultShim( String defaultShim ) {
    this.defaultShim = defaultShim;
  }
}
