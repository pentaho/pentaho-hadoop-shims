package org.pentaho.oozie.shim.api;

import java.util.Properties;

public interface OozieClient {
  public String getClientBuildVersion();

  public boolean hasAppPath( Properties properties );

  public String getProtocolUrl() throws OozieClientException;

  public void validateWSVersion() throws OozieClientException;

  public OozieJob run( Properties conf ) throws OozieClientException;

  public OozieJob getJob( String jobId );
}
