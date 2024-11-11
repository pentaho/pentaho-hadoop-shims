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
package org.pentaho.big.data.impl.shim;

import org.osgi.framework.BundleContext;
import org.pentaho.hadoop.shim.api.HadoopClientServices;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceFactory;
import org.pentaho.hadoop.shim.spi.HadoopShim;

public class HadoopClientServicesFactory implements NamedClusterServiceFactory<HadoopClientServices> {
  protected final HadoopShim hadoopShim;
  private BundleContext bundleContext;

  public HadoopClientServicesFactory( HadoopShim hadoopShim, BundleContext bundleContext ) {
    this.hadoopShim = hadoopShim;
    this.bundleContext = bundleContext;
  }

  @Override
  public Class<HadoopClientServices> getServiceClass() {
    return HadoopClientServices.class;
  }

  @Override
  public boolean canHandle( NamedCluster namedCluster ) {
    return true;
  }

  @Override
  public HadoopClientServices create( NamedCluster namedCluster ) {
    return new HadoopClientServicesImpl( namedCluster, hadoopShim, bundleContext );
  }
}
