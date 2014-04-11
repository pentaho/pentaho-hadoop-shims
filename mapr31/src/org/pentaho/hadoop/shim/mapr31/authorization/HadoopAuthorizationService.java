package org.pentaho.hadoop.shim.mapr31.authorization;

import org.pentaho.hadoop.shim.spi.PentahoHadoopShim;

public interface HadoopAuthorizationService {
  
  public <T extends PentahoHadoopShim> T getShim(Class<T> clazz);
}
