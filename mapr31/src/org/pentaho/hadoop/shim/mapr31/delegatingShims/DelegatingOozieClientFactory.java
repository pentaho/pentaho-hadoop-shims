package org.pentaho.hadoop.shim.mapr31.delegatingShims;

import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.mapr31.authorization.HadoopAuthorizationService;
import org.pentaho.hadoop.shim.mapr31.authorization.HasHadoopAuthorizationService;
import org.pentaho.oozie.shim.api.OozieClient;
import org.pentaho.oozie.shim.api.OozieClientFactory;

public class DelegatingOozieClientFactory implements OozieClientFactory, HasHadoopAuthorizationService {
  private OozieClientFactory delegate;

  @Override
  public ShimVersion getVersion() {
    return delegate.getVersion();
  }

  @Override
  public OozieClient create( String oozieUrl ) {
    return delegate.create( oozieUrl );
  }

  @Override
  public void setHadoopAuthorizationService( HadoopAuthorizationService hadoopAuthorizationService ) throws Exception {
    delegate = hadoopAuthorizationService.getShim( OozieClientFactory.class );
  }

}
