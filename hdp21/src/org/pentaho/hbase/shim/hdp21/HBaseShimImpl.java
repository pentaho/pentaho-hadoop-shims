package org.pentaho.hbase.shim.hdp21;

import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hbase.shim.spi.HBaseConnection;
import org.pentaho.hbase.shim.spi.HBaseShim;

public class HBaseShimImpl extends HBaseShim {

  @Override
  public ShimVersion getVersion() {
    return new ShimVersion( 1, 0 );
  }

  public HBaseConnection getHBaseConnection() {
    return new HBaseConnectionImpl();
  }

}
