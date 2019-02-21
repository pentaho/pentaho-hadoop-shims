package org.pentaho.hbase.shim.common;

import org.apache.hadoop.conf.Configuration;
import org.pentaho.hbase.factory.HBaseClientFactory;

public interface IHBaseClientFactoryGetter {
  HBaseClientFactory getHBaseClientFactory( Configuration configuration );
}
