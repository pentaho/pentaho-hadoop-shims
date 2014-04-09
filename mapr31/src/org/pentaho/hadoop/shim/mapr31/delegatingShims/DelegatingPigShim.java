package org.pentaho.hadoop.shim.mapr31.delegatingShims;

import java.net.URL;
import java.util.List;
import java.util.Properties;

import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.mapr31.authorization.HadoopAuthorizationService;
import org.pentaho.hadoop.shim.mapr31.authorization.HasHadoopAuthorizationService;
import org.pentaho.hadoop.shim.spi.PigShim;

public class DelegatingPigShim implements PigShim, HasHadoopAuthorizationService {
  private PigShim delegate;
  
  @Override
  public void setHadoopAuthorizationService( HadoopAuthorizationService hadoopAuthorizationService ) {
    delegate = hadoopAuthorizationService.getPigShim();
  }

  @Override
  public ShimVersion getVersion() {
    return delegate.getVersion();
  }

  @Override
  public boolean isLocalExecutionSupported() {
    return delegate.isLocalExecutionSupported();
  }

  @Override
  public void configure( Properties properties, Configuration configuration ) {
    delegate.configure( properties, configuration );
  }

  @Override
  public String substituteParameters( URL pigScript, List<String> paramList ) throws Exception {
    return delegate.substituteParameters( pigScript, paramList );
  }

  @Override
  public int[] executeScript( String pigScript, ExecutionMode mode, Properties properties ) throws Exception {
    return delegate.executeScript( pigScript, mode, properties );
  }
  
  
}
