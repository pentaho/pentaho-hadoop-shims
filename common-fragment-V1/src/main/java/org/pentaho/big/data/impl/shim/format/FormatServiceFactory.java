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
package org.pentaho.big.data.impl.shim.format;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceFactory;
import org.pentaho.hadoop.shim.api.format.FormatService;
import org.pentaho.hadoop.shim.spi.FormatShim;

public class FormatServiceFactory implements NamedClusterServiceFactory<FormatService> {
  private final FormatShim formatShim;

  public FormatServiceFactory( FormatShim formatShim ) {
    this.formatShim = formatShim;
  }

  @Override public Class<FormatService> getServiceClass() {
    return FormatService.class;
  }

  @Override public boolean canHandle( NamedCluster namedCluster ) {
    return true;
  }

  @Override public FormatService create( NamedCluster namedCluster ) {
    return new FormatServiceImpl( namedCluster, formatShim );
  }
}
