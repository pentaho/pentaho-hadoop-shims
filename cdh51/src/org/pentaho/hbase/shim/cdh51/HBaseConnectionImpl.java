package org.pentaho.hbase.shim.cdh51;

import org.pentaho.hbase.shim.common.CommonHBaseConnection;
import org.pentaho.hbase.shim.cdh51.wrapper.HBaseConnectionInterface;

public class HBaseConnectionImpl extends CommonHBaseConnection implements HBaseConnectionInterface {

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
    return Class.forName( "org.pentaho.hbase.shim.cdh51.DeserializedNumericComparator" );
  }

  @Override
  public Class<?> getDeserializedBooleanComparatorClass() throws ClassNotFoundException {
    return Class.forName( "org.pentaho.hbase.shim.cdh51.DeserializedBooleanComparator" );
  }
}
