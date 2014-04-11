package org.pentaho.oozie.shim.api;

import org.pentaho.hadoop.shim.spi.PentahoHadoopShim;

public interface OozieClientFactory extends PentahoHadoopShim {
  public OozieClient create( String oozieUrl );
}
