package org.pentaho.hbase.shim.mapr31.wrapper;

import org.apache.hadoop.conf.Configuration;
import org.pentaho.hadoop.shim.spi.PentahoHadoopShim;
import org.pentaho.hbase.shim.spi.HBaseConnection;

public interface HBaseShimInterface extends PentahoHadoopShim {
  public HBaseConnection getHBaseConnection();
  
  public void setInfo( Configuration configuration );
}
