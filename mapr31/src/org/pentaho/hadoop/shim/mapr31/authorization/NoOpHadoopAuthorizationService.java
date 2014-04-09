package org.pentaho.hadoop.shim.mapr31.authorization;

import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.PigShim;
import org.pentaho.hadoop.shim.spi.SnappyShim;
import org.pentaho.hadoop.shim.spi.SqoopShim;
import org.pentaho.hbase.shim.mapr31.wrapper.HBaseShimInterface;

public class NoOpHadoopAuthorizationService implements HadoopAuthorizationService {

  @Override
  public HadoopShim getHadoopShim() {
    return new org.pentaho.hadoop.shim.mapr31.HadoopShim();
  }

  @Override
  public PigShim getPigShim() {
    return new org.pentaho.hadoop.shim.mapr31.PigShim();
  }

  @Override
  public SnappyShim getSnappyShim() {
    return new org.pentaho.hadoop.shim.mapr31.SnappyShim();
  }

  @Override
  public SqoopShim getSqoopShim() {
    return new org.pentaho.hadoop.shim.common.CommonSqoopShim();
  }

  @Override
  public HBaseShimInterface getHBaseShimInterface() {
    return new org.pentaho.hbase.shim.mapr31.MapRHBaseShim();
  }
}
