package org.pentaho.oozie.shim.mapr31;

import java.util.Arrays;
import java.util.HashSet;

import org.apache.oozie.client.AuthOozieClient;
import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.oozie.shim.api.OozieClient;
import org.pentaho.oozie.shim.api.OozieClientFactory;
import org.pentaho.oozie.shim.api.OozieJob;

public class OozieClientFactoryImpl implements OozieClientFactory {
  private final String doAsUser;

  public OozieClientFactoryImpl( String doAsUser ) {
    this.doAsUser = doAsUser;
  }

  @Override
  public ShimVersion getVersion() {
    return new ShimVersion( 0, 0 );
  }

  @Override
  public OozieClient create( String oozieUrl ) {
    return OozieImpersonationInvocationHandler.forObject( new OozieClientImpl( new AuthOozieClient( oozieUrl ) ),
        new HashSet<Class<?>>( Arrays.<Class<?>> asList( OozieClient.class, OozieJob.class ) ), doAsUser );
  }
}
