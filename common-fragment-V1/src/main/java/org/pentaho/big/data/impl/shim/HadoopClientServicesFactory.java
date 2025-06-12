package org.pentaho.big.data.impl.shim;

import org.pentaho.hadoop.shim.api.HadoopClientServices;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceFactory;
import org.pentaho.hadoop.shim.spi.HadoopShim;

public class HadoopClientServicesFactory implements NamedClusterServiceFactory<HadoopClientServices> {
  protected final HadoopShim hadoopShim;

  public HadoopClientServicesFactory( HadoopShim hadoopShim ) {
    this.hadoopShim = hadoopShim;
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
    return new HadoopClientServicesImpl( namedCluster, hadoopShim );
  }
}
