package org.pentaho.oozie.shim.api;

public interface OozieJob {
  public String getId();

  public boolean isRunning() throws OozieClientException;

  public boolean didSucceed() throws OozieClientException;
  
  public String getJobLog() throws OozieClientException;
}
