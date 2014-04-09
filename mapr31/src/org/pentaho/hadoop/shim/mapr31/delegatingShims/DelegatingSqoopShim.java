package org.pentaho.hadoop.shim.mapr31.delegatingShims;

import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.mapr31.authorization.HadoopAuthorizationService;
import org.pentaho.hadoop.shim.mapr31.authorization.HasHadoopAuthorizationService;
import org.pentaho.hadoop.shim.spi.SqoopShim;

public class DelegatingSqoopShim implements SqoopShim, HasHadoopAuthorizationService {
  private SqoopShim delegate;
  
  @Override
  public void setHadoopAuthorizationService( HadoopAuthorizationService hadoopAuthorizationService ) {
    delegate = hadoopAuthorizationService.getSqoopShim();
  }

  @Override
  public int runTool( String[] args, Configuration c ) {
    return delegate.runTool( args, c );
  }

  @Override
  public ShimVersion getVersion() {
    return delegate.getVersion();
  }
}
