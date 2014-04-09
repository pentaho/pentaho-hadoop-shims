package org.pentaho.hadoop.shim.mapr31.authorization;

import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.PigShim;
import org.pentaho.hadoop.shim.spi.SnappyShim;
import org.pentaho.hadoop.shim.spi.SqoopShim;
import org.pentaho.hbase.shim.mapr31.wrapper.HBaseShimInterface;

public interface HadoopAuthorizationService {
  public HadoopShim getHadoopShim();
  
  public PigShim getPigShim();
  
  public SnappyShim getSnappyShim();
  
  public SqoopShim getSqoopShim();
  
  public HBaseShimInterface getHBaseShimInterface();
}
