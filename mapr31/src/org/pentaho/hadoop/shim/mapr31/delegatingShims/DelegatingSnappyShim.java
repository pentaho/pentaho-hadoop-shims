package org.pentaho.hadoop.shim.mapr31.delegatingShims;

import java.io.InputStream;
import java.io.OutputStream;

import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.mapr31.authorization.HadoopAuthorizationService;
import org.pentaho.hadoop.shim.mapr31.authorization.HasHadoopAuthorizationService;
import org.pentaho.hadoop.shim.spi.SnappyShim;

public class DelegatingSnappyShim implements SnappyShim, HasHadoopAuthorizationService {
  private SnappyShim delegate;

  @Override
  public void setHadoopAuthorizationService( HadoopAuthorizationService hadoopAuthorizationService ) {
    delegate = hadoopAuthorizationService.getSnappyShim();
  }

  @Override
  public boolean isHadoopSnappyAvailable() {
    return delegate.isHadoopSnappyAvailable();
  }

  @Override
  public ShimVersion getVersion() {
    return delegate.getVersion();
  }

  @Override
  public InputStream getSnappyInputStream( InputStream in ) throws Exception {
    return delegate.getSnappyInputStream( in );
  }

  @Override
  public InputStream getSnappyInputStream( int bufferSize, InputStream in ) throws Exception {
    return delegate.getSnappyInputStream( bufferSize, in );
  }

  @Override
  public OutputStream getSnappyOutputStream( OutputStream out ) throws Exception {
    return delegate.getSnappyOutputStream( out );
  }

  @Override
  public OutputStream getSnappyOutputStream( int bufferSize, OutputStream out ) throws Exception {
    return delegate.getSnappyOutputStream( bufferSize, out );
  }
}
