package org.pentaho.hadoop.shim.mapr31.delegatingShims;

import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.mapr31.authorization.HadoopAuthorizationService;
import org.pentaho.hadoop.shim.mapr31.authorization.HasHadoopAuthorizationService;
import org.pentaho.hbase.shim.mapr31.wrapper.HBaseShimInterface;
import org.pentaho.hbase.shim.spi.HBaseConnection;
import org.pentaho.hbase.shim.spi.HBaseShim;

public class DelegatingHBaseShim extends HBaseShim implements HasHadoopAuthorizationService {
  private HBaseShimInterface delegate;

  @Override
  public ShimVersion getVersion() {
    return delegate.getVersion();
  }

  @Override
  public void setHadoopAuthorizationService( HadoopAuthorizationService hadoopAuthorizationService ) {
    delegate = hadoopAuthorizationService.getHBaseShimInterface();
  }

  @Override
  public HBaseConnection getHBaseConnection() {
    return delegate.getHBaseConnection();
  }
}
