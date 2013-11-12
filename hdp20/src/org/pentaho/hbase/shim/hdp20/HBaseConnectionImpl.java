package org.pentaho.hbase.shim.hdp20;

import org.pentaho.hbase.shim.common.CommonHBaseConnection;

public class HBaseConnectionImpl extends CommonHBaseConnection {

  @Override
  public Class<?> getByteArrayComparableClass() throws ClassNotFoundException {
    return Class.forName( "org.apache.hadoop.hbase.filter.ByteArrayComparable" );
  }

  @Override
  public Class<?> getCompressionAlgorithmClass() throws ClassNotFoundException {
    return Class.forName( "org.apache.hadoop.hbase.io.compress.Compression.Algorithm" );
  }

  @Override
  public Class<?> getBloomTypeClass() throws ClassNotFoundException {
    return Class.forName( "org.apache.hadoop.hbase.regionserver.BloomType" );
  }

  @Override
  public Class<?> getDeserializedNumericComparatorClass() throws ClassNotFoundException {
    return Class.forName( "org.apache.hbase.shim.hdp20.DeserializedNumericComparator" );
  }

  @Override
  public Class<?> getDeserializedBooleanComparatorClass() throws ClassNotFoundException {
    return Class.forName( "org.apache.hbase.shim.hdp20.DeserializedBooleanComparator" );
  }
}
